from datetime import datetime, timezone

import pytest
from inveniautils.stream import SeekableStream

from datawarehouse import exceptions
from datawarehouse.interface import DataWarehouseInterface as API
from tests.aws_setup import get_warehouse_sesh, mock_start, mock_stop, setup_resources
from tests.utils import get_streams, register_test_collections


@pytest.fixture()
def warehouse():
    mock_start()
    setup_resources()
    wh = get_warehouse_sesh()
    register_test_collections(wh)
    yield wh
    mock_stop()


def _generate_parsed_file(content, source_metadata):
    parsed = SeekableStream(content, **source_metadata)
    parsed.metadata[API.CONTENT_START_FIELD] = datetime.now(timezone.utc)
    return parsed


def _store_source_files(warehouse):
    """ Helper method to store some source files into the test collection. """
    # pkey: "url"  -  type_map: {url: STR, filename: STR}
    warehouse.select_collection("test_collection", database="test_database")
    # This loads in 4 SeekableStreams with the same Primary Key but unique content.
    files = get_streams("test_database", "test_collection")
    assert len(files) == 4
    versions = []
    file_key = warehouse.get_primary_key(files[0].metadata)
    # Store all versions in the warehouse.
    for file in files:
        response = warehouse.store(file)
        assert response["primary_key"] == file_key
        assert response["status_code"] == warehouse.STATUS.SUCCESS
        versions.append(response[API.VERSION_FIELD])
    assert len(set(versions)) == len(versions)
    return file_key, versions


def test_store_and_retrieve_with_default_parser(warehouse):
    file_key, versions = _store_source_files(warehouse)

    parsed_content = []

    # store and retrieve parsed files for all source file versions
    for version in versions:
        source_metadata = warehouse.retrieve(file_key, version, metadata_only=True)
        content = "parsed content for" + str(version)
        parsed = _generate_parsed_file(content, source_metadata)
        response = warehouse.store(parsed, parsed_file=True)
        assert response["primary_key"] == file_key
        assert response["source_version"] == version
        assert response["status_code"] == warehouse.STATUS.SUCCESS
        assert response["parser_name"] == warehouse.default_parser_name
        parsed_content.append(content)

    assert len(set(parsed_content)) == len(versions)
    iter_parsed = iter(parsed_content)

    # now retrieve the file and check its contents
    for version in versions:
        stored = warehouse.retrieve(file_key, version, parsed_file=True)
        assert stored.read() == next(iter_parsed)
        assert warehouse.get_source_version(stored.metadata) == version

    # test retrieve_versions for parsed files
    stored_versions = warehouse.retrieve_versions(file_key, parsed_file=True)
    for version in sorted(versions, reverse=True):
        stored = next(stored_versions)
        assert stored.read() == "parsed content for" + str(version)
        assert stored.metadata[API.VERSION_FIELD] == version


def test_store_and_retrieve_with_specified_parser(warehouse):
    file_key, versions = _store_source_files(warehouse)

    # the "test_collection" collection has 2 parsers registered
    assert len(warehouse.available_parsers) > 1

    parsed_content = []

    for parser in warehouse.available_parsers:
        for version in versions:
            source_metadata = warehouse.retrieve(file_key, version, metadata_only=True)
            content = "parsed content for" + str(parser) + str(version)
            parsed = _generate_parsed_file(content, source_metadata)
            response = warehouse.store(parsed, parsed_file=True, parser_name=parser)
            assert response["primary_key"] == file_key
            assert response["source_version"] == version
            assert response["status_code"] == warehouse.STATUS.SUCCESS
            assert response["parser_name"] == parser
            parsed_content.append(content)

    assert len(set(parsed_content)) == len(warehouse.available_parsers) * len(versions)
    iter_parsed = iter(parsed_content)

    # now retrieve the file and check its contents
    for parser in warehouse.available_parsers:
        for version in versions:
            stored = warehouse.retrieve(
                file_key, version, parsed_file=True, parser_name=parser
            )
            assert stored.read() == next(iter_parsed)
            assert warehouse.get_source_version(stored.metadata) == version

    # test invalid parser
    version = versions[0]
    source_metadata = warehouse.retrieve(file_key, version, metadata_only=True)
    parsed = _generate_parsed_file("some-content", source_metadata)

    invalid_parser = "invalid_parser"

    with pytest.raises(exceptions.ArgumentError):
        warehouse.store(parsed, parsed_file=True, parser_name=invalid_parser)

    with pytest.raises(exceptions.ArgumentError):
        warehouse.retrieve(file_key, parsed_file=True, parser_name=invalid_parser)

    with pytest.raises(exceptions.ArgumentError):
        warehouse.retrieve(
            file_key, version, parsed_file=True, parser_name=invalid_parser
        )


def test_retrieve_missing_parsed_file(warehouse):
    warehouse.select_collection("test_collection", database="test_database")
    # both source file and parsed file does not exist
    file_key = "random-key"
    stored = warehouse.retrieve(file_key)
    assert stored is None
    stored = warehouse.retrieve(file_key, parsed_file=True)
    assert stored is None

    # store some source files.
    file_key, versions = _store_source_files(warehouse)

    # Source file exist, but parsed file does not.
    stored = warehouse.retrieve(file_key)
    assert stored is not None
    stored = warehouse.retrieve(file_key, parsed_file=True)
    assert stored is None

    # Source file version exist, but parsed file does not.
    stored = warehouse.retrieve(file_key, versions[0])
    assert stored is not None
    stored = warehouse.retrieve(file_key, versions[0], parsed_file=True)
    assert stored is None


def test_store_with_content_start(warehouse):
    file_key, versions = _store_source_files(warehouse)

    # generate a parsed file for storing
    source_metadata = warehouse.retrieve(file_key, versions[0], metadata_only=True)
    parsed = _generate_parsed_file("parsed_content" + str(versions[0]), source_metadata)

    # pop the required content_start field
    content_start = parsed.metadata.pop(API.CONTENT_START_FIELD)
    with pytest.raises(exceptions.MetadataError):
        warehouse.store(parsed, parsed_file=True)

    # add the content_start field back and try again
    parsed.metadata[API.CONTENT_START_FIELD] = content_start
    response = warehouse.store(parsed, parsed_file=True)
    assert response["status_code"] == warehouse.STATUS.SUCCESS
    assert response["parser_name"] == warehouse.default_parser_name
