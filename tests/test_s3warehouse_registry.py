import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import pytz
from freezegun import freeze_time
from inveniautils.configuration import Configuration

from datawarehouse import exceptions, implementations
from tests.aws_setup import (
    PARSED_BUCKET,
    REGION,
    REGISTRY_TABLE,
    SOURCE_BUCKET,
    SOURCE_TABLE,
    get_warehouse_sesh,
    mock_start,
    mock_stop,
    setup_resources,
)
from tests.utils import register_test_collections


@pytest.fixture()
def warehouse():
    mock_start()
    setup_resources()
    yield get_warehouse_sesh()
    mock_stop()


@pytest.fixture()
def freezer():
    frozen_time = freeze_time("2020-01-01 00:00:00-00:00")
    yield frozen_time.start()
    frozen_time.stop()


def test_instantiate():
    # Instantiate from default config file path.
    path_str = str(Path("tests/files/warehouse_configs.yaml"))
    os.environ[implementations.CONFIG_PATH_VAR] = path_str
    warehouse = implementations.S3Warehouse()
    assert warehouse._region == "test-file-region"
    assert warehouse._registry_table == "test-file-registry"
    assert warehouse._source_table == "test-file-index"
    assert warehouse._source_bucket == "test-file-source"
    assert warehouse._parsed_bucket == "test-file-parsed"
    assert warehouse._role_arn == "test-file-arn"
    assert warehouse._sesh_duration == 54321
    assert warehouse._cache_ttl == timedelta(seconds=600)

    # If a custom config object is passed in, use that instead.
    cfg = Configuration(str(Path("tests/files/warehouse_configs2.yaml")))
    warehouse = implementations.S3Warehouse(config=cfg)
    assert warehouse._region == "test-file-region2"
    assert warehouse._registry_table == "test-file-registry2"
    assert warehouse._source_table == "test-file-index2"
    assert warehouse._source_bucket == "test-file-source2"
    assert warehouse._parsed_bucket == "test-file-parsed2"
    assert warehouse._role_arn is None
    assert warehouse._sesh_duration is None
    assert warehouse._cache_ttl == timedelta(seconds=300)  # the default when not given

    # If kw args are passed in, overwrite stuff in config file.
    warehouse = implementations.S3Warehouse(
        config=cfg,
        source_table_name=SOURCE_TABLE,
        source_bucket_name=SOURCE_BUCKET,
        role_arn="custom-role",
    )
    assert warehouse._region == "test-file-region2"
    assert warehouse._registry_table == "test-file-registry2"
    assert warehouse._source_table == SOURCE_TABLE
    assert warehouse._source_bucket == SOURCE_BUCKET
    assert warehouse._parsed_bucket == "test-file-parsed2"
    assert warehouse._role_arn == "custom-role"
    assert warehouse._sesh_duration == 3600  # the default when not given
    assert warehouse._cache_ttl == timedelta(seconds=300)

    # Instantiate from kw args and do not default to config file
    warehouse = implementations.S3Warehouse(
        region_name=REGION,
        registry_table_name=REGISTRY_TABLE,
        source_table_name=SOURCE_TABLE,
        source_bucket_name=SOURCE_BUCKET,
        parsed_bucket_name=PARSED_BUCKET,
        cache_ttl=0,
        load_defaults=False,
    )
    assert warehouse._region == REGION
    assert warehouse._registry_table == REGISTRY_TABLE
    assert warehouse._source_table == SOURCE_TABLE
    assert warehouse._source_bucket == SOURCE_BUCKET
    assert warehouse._parsed_bucket == PARSED_BUCKET
    assert warehouse._role_arn is None  # does not fall back to the config file
    assert warehouse._sesh_duration is None
    assert warehouse._cache_ttl == timedelta(0)

    # Instantiate with missing required args
    with pytest.raises(exceptions.ArgumentError):
        warehouse = implementations.S3Warehouse(
            region_name=REGION,
            registry_table_name=REGISTRY_TABLE,
            source_table_name=SOURCE_TABLE,
            load_defaults=False,
        )


def test_empty_warehouse(warehouse):
    # Warehouse is empty
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse.list_databases() == []

    # No database selected yet, can't list collections
    with pytest.raises(exceptions.OperationError):
        warehouse.list_collections()

    # Selecting a non registered collection fails
    with pytest.raises(exceptions.OperationError):
        warehouse.select_collection("realtime_price", database="miso")

    # Selecting invalid db and collection on instantiation fails.
    with pytest.raises(exceptions.OperationError):
        get_warehouse_sesh("miso", "realtime")

    # These will all fails with OperationError because no db/collection is selected.
    invalid_operations = [
        "database",
        "collection",
        "primary_key_fields",
        "required_metadata_fields",
        "metadata_type_map",
        "default_parser_name",
        "default_parser_pkey_fields",
        "default_parser_type_map",
        "default_parser_timezone",
        "available_parsers",
    ]
    for op in invalid_operations:
        with pytest.raises(exceptions.OperationError):
            getattr(warehouse, op)


def test_register_new_sources(warehouse):
    # warehouse is empty
    assert warehouse.list_databases_and_collections() == {}

    # Primary key not defined
    with pytest.raises(exceptions.ArgumentError):
        warehouse.update_source_registry(
            database="miso",
            collection="load",
            required_metadata_fields="key2",
            metadata_type_map={"key1": datetime, "key2": int},
        )

    # register new collection
    warehouse.update_source_registry(
        database="miso",
        collection="load",
        primary_key_fields="key1",
        required_metadata_fields="key2",
        metadata_type_map={"key1": datetime, "key2": int},
    )

    # listing the new database and collection
    assert warehouse.list_databases_and_collections() == {"miso": ["load"]}
    assert warehouse.list_databases() == ["miso"]

    # registering a collection does not automatically select it for use.
    with pytest.raises(exceptions.OperationError):
        warehouse.select_collection("load")  # no database selected yet
    with pytest.raises(exceptions.OperationError):
        warehouse.select_collection("load", database="misa")  # ops a typo
    with pytest.raises(exceptions.OperationError):
        warehouse.database  # db still not selected
    with pytest.raises(exceptions.OperationError):
        warehouse.collection  # collection still not selected

    # select the collections and verify collection attributes
    warehouse.select_collection("load", database="miso")
    assert warehouse.list_collections() == ["load"]
    assert warehouse.database == "miso"
    assert warehouse.collection == "load"
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1", "key2")
    assert warehouse.metadata_type_map == {"key1": datetime, "key2": int}

    # Since no parsers are registered with the collecetion, these will fail.
    assert warehouse.available_parsers == {}
    invalid_operations = [
        "default_parser_name",
        "default_parser_pkey_fields",
        "default_parser_type_map",
        "default_parser_timezone",
    ]
    for op in invalid_operations:
        with pytest.raises(exceptions.OperationError):
            getattr(warehouse, op)

    # register another collection in the same db
    warehouse.update_source_registry(
        database="miso",
        collection="realtime",
        primary_key_fields=("key1", "key2"),
        required_metadata_fields=("key3", "key4"),
        metadata_type_map={
            "key1": datetime,
            "key2": int,
            "key3": str,
            "key4": float,
        },
    )
    assert warehouse.list_databases_and_collections() == {"miso": ["load", "realtime"]}
    assert warehouse.list_collections() == ["load", "realtime"]

    # switch collection and check the new collection's attributes
    assert warehouse.collection == "load"
    warehouse.select_collection("realtime")
    assert warehouse.collection == "realtime"
    assert warehouse.database == "miso"  # still the same db
    assert warehouse.primary_key_fields == ("key1", "key2")
    assert warehouse.required_metadata_fields == ("key1", "key2", "key3", "key4")
    assert warehouse.metadata_type_map == {
        "key1": datetime,
        "key2": int,
        "key3": str,
        "key4": float,
    }

    # register a new collection in a new db
    warehouse.update_source_registry(
        database="ercot",
        collection="dayahead",
        primary_key_fields="key1",
        metadata_type_map={"key1": datetime},
    )
    assert warehouse.list_databases_and_collections() == {
        "miso": ["load", "realtime"],
        "ercot": ["dayahead"],
    }

    # switch collection and check the new collection's attributes
    with pytest.raises(exceptions.OperationError):
        warehouse.select_collection("dayahead")  # need to switch database as well
    warehouse.select_collection("dayahead", database="ercot")
    assert warehouse.database == "ercot"
    assert warehouse.collection == "dayahead"
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1",)
    assert warehouse.metadata_type_map == {"key1": datetime}

    # Ensure that the registry persists across sessions, and select collection on init
    new_warehouse = get_warehouse_sesh("ercot", "dayahead")
    assert new_warehouse.list_databases_and_collections() == {
        "miso": ["load", "realtime"],
        "ercot": ["dayahead"],
    }
    assert new_warehouse.database == "ercot"
    assert new_warehouse.collection == "dayahead"


def test_update_existing_source(warehouse):
    # warehouse is empty
    assert warehouse.list_databases_and_collections() == {}

    # register new collection
    warehouse.update_source_registry(
        database="miso",
        collection="load",
        primary_key_fields="key1",
        required_metadata_fields="key2",
        metadata_type_map={"key1": datetime, "key2": int},
    )
    warehouse.select_collection("load", database="miso")

    # try to update primary keys, this will fail.
    with pytest.raises(exceptions.ArgumentError):
        warehouse.update_source_registry("miso", "load", ("key1", "keys3"))
    # keys not changed
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1", "key2")

    # Update the 'other required fields', this will replace existing keys, but metadata
    # type map for previous keys will not be removed.
    warehouse.update_source_registry(
        database="miso",
        collection="load",
        required_metadata_fields=("key3", "key4"),
        metadata_type_map={"key3": float, "key4": float},
    )
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1", "key3", "key4")
    assert warehouse.metadata_type_map == {
        "key1": datetime,
        "key2": int,
        "key3": float,
        "key4": float,
    }

    # Remove all 'other required fields', and update "key2"'s type.
    warehouse.update_source_registry(
        database="miso",
        collection="load",
        required_metadata_fields=(),  # replace with empty
        metadata_type_map={"key2": float},
    )
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1",)
    assert warehouse.metadata_type_map == {
        "key1": datetime,
        "key2": float,
        "key3": float,
        "key4": float,
    }

    # update fails because typemap for the a new field is not provided
    with pytest.raises(exceptions.ArgumentError):
        warehouse.update_source_registry(
            database="miso",
            collection="load",
            required_metadata_fields=("keys5", "key6"),
            metadata_type_map={"key5": float},
        )
    # keys not chnaged
    assert warehouse.primary_key_fields == ("key1",)
    assert warehouse.required_metadata_fields == ("key1",)
    assert warehouse.metadata_type_map == {
        "key1": datetime,
        "key2": float,
        "key3": float,
        "key4": float,
    }


def test_register_and_update_parsers(warehouse):
    # warehouse is empty
    assert warehouse.list_databases_and_collections() == {}

    # register new collection
    warehouse.update_source_registry(
        database="miso",
        collection="load",
        primary_key_fields="key1",
        metadata_type_map={"key1": datetime},
    )
    warehouse.select_collection("load", database="miso")
    assert warehouse.available_parsers == {}

    # Primary key not defined
    with pytest.raises(exceptions.ArgumentError):
        warehouse.update_parsed_registry(
            database="miso",
            collection="load",
            parser_name="parser1",
            row_type_map={"key1": datetime, "key2": int},
            timezone=pytz.timezone("America/Chicago"),
        )

    # type map is not complete
    with pytest.raises(exceptions.ArgumentError):
        warehouse.update_parsed_registry(
            database="miso",
            collection="load",
            parser_name="parser1",
            primary_key_fields=("key1", "key2", "key3"),
            row_type_map={"key1": datetime, "key2": int},
            timezone=pytz.timezone("America/Chicago"),
        )

    parsers = {
        "first_parser": {
            "primary_key_fields": ("key1",),
            "row_type_map": {"key1": datetime, "key2": int},
            "timezone": pytz.timezone("America/New_York"),
        },
        "second_parser": {
            "primary_key_fields": ("key1", "key2"),
            "row_type_map": {"key1": int, "key2": str, "key3": int},
            "timezone": pytz.timezone("America/Chicago"),
        },
        "second_parser_updated": {
            "primary_key_fields": ("key1", "key2", "key10"),
            "row_type_map": {"key1": int, "key2": str, "key10": str},
            "timezone": pytz.timezone("Zulu"),
        },
    }

    # the first registered parser is auto default
    par1 = "first_parser"
    warehouse.update_parsed_registry(
        database="miso",
        collection="load",
        parser_name=par1,
        **parsers[par1],
    )
    assert warehouse.default_parser_name == par1
    assert warehouse.default_parser_pkey_fields == parsers[par1]["primary_key_fields"]
    assert warehouse.default_parser_type_map == parsers[par1]["row_type_map"]
    assert warehouse.default_parser_timezone == parsers[par1]["timezone"]
    assert par1 in warehouse.available_parsers

    # second parser, not auto default
    par2 = "second_parser"
    warehouse.update_parsed_registry(
        database="miso",
        collection="load",
        parser_name=par2,
        **parsers[par2],
    )
    assert par2 in warehouse.available_parsers
    # default parser still not changed
    assert warehouse.default_parser_name == par1
    assert warehouse.default_parser_pkey_fields == parsers[par1]["primary_key_fields"]
    assert warehouse.default_parser_type_map == parsers[par1]["row_type_map"]
    assert warehouse.default_parser_timezone == parsers[par1]["timezone"]

    # promote parser2
    warehouse.update_parsed_registry(
        database="miso",
        collection="load",
        parser_name=par2,
        promote_default=True,
    )
    # default parser now changed
    assert warehouse.default_parser_name == par2
    assert warehouse.default_parser_pkey_fields == parsers[par2]["primary_key_fields"]
    assert warehouse.default_parser_type_map == parsers[par2]["row_type_map"]
    assert warehouse.default_parser_timezone == parsers[par2]["timezone"]

    # Update parser 2 with different attributes
    par2up = "second_parser_updated"
    warehouse.update_parsed_registry(
        database="miso",
        collection="load",
        parser_name=par2,
        **parsers[par2up],
    )
    # default parser still not changed
    assert warehouse.default_parser_name == "second_parser"
    assert warehouse.default_parser_pkey_fields == parsers[par2up]["primary_key_fields"]
    assert warehouse.default_parser_type_map == parsers[par2up]["row_type_map"]
    assert warehouse.default_parser_timezone == parsers[par2up]["timezone"]

    # register parser for non-existent collection, this will fail.
    with pytest.raises(exceptions.OperationError):
        warehouse.update_parsed_registry(
            database="miso",
            collection="hyper_load",
            parser_name=par1,
            **parsers[par1],
        )


def test_registry_cache(warehouse):
    # do a table scan and show that the warehouse is empty
    assert warehouse._last_scan is None
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan is not None
    first_scan = warehouse._last_scan

    # this registers a bunch of collections using a different session
    register_test_collections(get_warehouse_sesh())

    # default session cache has a 300s ttl, no new scan
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan == first_scan

    # The select_collection() operation never uses the cache nor does it do a scan,
    # it directly gets the target collection.
    warehouse.select_collection("realtime_price", database="caiso")
    assert len(warehouse.list_databases_and_collections()) == 1
    assert warehouse._last_scan == first_scan  # still no new scan

    # check that collection attributes uses the cache
    type_map = warehouse.metadata_type_map
    # update the collection using a different sesh
    new_sesh = get_warehouse_sesh()
    added_map = {"new_key": datetime}
    new_tz = pytz.timezone("Zulu")
    new_sesh.update_source_registry(
        database=warehouse.database,
        collection=warehouse.collection,
        metadata_type_map=added_map,
    )
    new_sesh.update_parsed_registry(
        database=warehouse.database,
        collection=warehouse.collection,
        parser_name=warehouse.default_parser_name,
        timezone=new_tz,
    )
    # the previous session is still using the cache
    new_map = {**type_map, **added_map}
    assert warehouse.metadata_type_map != new_map
    assert warehouse.default_parser_timezone != new_tz
    # reselect the collection to renew the cache
    warehouse.select_collection("realtime_price", database="caiso")
    assert warehouse.metadata_type_map == new_map
    assert warehouse.default_parser_timezone == new_tz

    # get a fresh session and try again
    warehouse = get_warehouse_sesh()
    assert len(warehouse.list_databases_and_collections()) > 1


def test_no_registry_cache(warehouse):
    # get a custom warehouse sesh with a 0s ttl cache (i.e. doesn't do caching)
    warehouse = get_warehouse_sesh(ttl=0)

    # Warehouse is empty
    assert warehouse._last_scan is None
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan is not None
    first_scan = warehouse._last_scan

    # this registers a bunch of collections using a different session
    register_test_collections(get_warehouse_sesh())

    # a new scan is done
    assert warehouse.list_databases_and_collections() != {}
    assert warehouse._last_scan != first_scan


# Note that the time freezer fixture is passed in.
def test_renew_registry_cache(warehouse, freezer):
    # Warehouse is empty
    assert warehouse._last_scan is None
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan is not None
    first_scan = warehouse._last_scan

    # this registers a bunch of collections using a different session
    register_test_collections(get_warehouse_sesh())

    # default session cache has a 300s ttl, no new scan
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan == first_scan

    # fast forward time to half of the TTL, still no new scan
    freezer.tick(warehouse._cache_ttl / 2)
    assert warehouse.list_databases_and_collections() == {}
    assert warehouse._last_scan == first_scan

    # fast forward time past the TTL, new scan is done
    freezer.tick(warehouse._cache_ttl)
    assert warehouse.list_databases_and_collections() != {}
    assert warehouse._last_scan != first_scan
