from datetime import datetime, timedelta, timezone

import inveniautils.timestamp as timestamp
import pytest
from inveniautils.datetime_range import DatetimeRange
from inveniautils.stream import SeekableStream

from datawarehouse import exceptions
from datawarehouse.interface import DataWarehouseInterface as API
from tests.dynamo_warehouse.aws_setup import (
    get_warehouse_sesh,
    mock_start,
    mock_stop,
    setup_resources,
)
from tests.utils import load_timestamps_2020, register_test_collections


start_key = API.CONTENT_START_FIELD
end_key = API.CONTENT_END_FIELD
release_key = API.RELEASE_FIELD


@pytest.fixture()
def warehouse():
    mock_start()
    setup_resources()
    wh = get_warehouse_sesh()
    register_test_collections(wh)
    yield wh
    mock_stop()


def _random_dtrs_2020(interval: timedelta):
    # 2020-01-01 to 2021-01-01
    random_timestamps = load_timestamps_2020()
    seconds = int(interval.total_seconds())
    for ts_random in random_timestamps:
        ts_floored = ts_random - ts_random % seconds
        dt_start = timestamp.to_datetime(ts_floored)
        yield DatetimeRange(dt_start, dt_start + interval)


def _store_random_files_2020(warehouse, interval: timedelta):
    """Helper method to store some random source files into the test collection."""
    warehouse.select_collection("test_query", database="test_database")
    file_ids = []

    for i, dtr in enumerate(_random_dtrs_2020(interval)):
        content = str(dtr)
        # url is the primary key for this collection.
        metadata = dict(url=content, retrieved_date=dtr.start, release_date=dtr.start)
        if i % 2 == 0:  # add content_start to every other file
            metadata["content_start"] = dtr.start
        if i % 8 != 0:  # don't add content_end to every 8th file
            metadata["content_end"] = dtr.end

        file = SeekableStream(content, **metadata)
        response = warehouse.store(file, force_store=True)
        assert response["status_code"] == warehouse.STATUS.SUCCESS
        file_ids.append((response["primary_key"], response[API.VERSION_FIELD]))

    assert len(set(file_ids)) == len(file_ids) == 300
    return file_ids


def test_query_content(warehouse):
    # stores 300 daily interval files from 2020-01-01 to 2020-01-11
    file_ids = set(_store_random_files_2020(warehouse, timedelta(days=1)))

    # If the ContentStartIndex is used, only entries with content_start will be returned
    all_results = list(warehouse.query_metadata_items(index=warehouse.INDEXES.CONTENT))
    assert len(all_results) < len(file_ids)

    # If no Index is specified, returns all entries from the collection regardless if
    # content start/end is available
    all_results = list(warehouse.query_metadata_items())
    assert len(all_results) == len(file_ids)

    # query for a specific range
    start = datetime(2020, 3, 3, tzinfo=timezone.utc)
    end = datetime(2020, 9, 6, tzinfo=timezone.utc)
    query_range = DatetimeRange(start, end)
    query_results = list(warehouse.query_metadata_items(query_range))
    query_result_ids = set(
        (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        for row in query_results
    )
    assert 0 < len(query_results) < len(file_ids)
    assert query_result_ids.issubset(file_ids)

    # check that the queried content is accurate.
    have_end = 0
    for row in query_results:
        assert row[start_key] <= end
        if end_key in row:
            assert start < row[end_key]
            have_end += 1
    # show that the query grabs files with missing content_end as well.
    assert 0 < have_end < len(query_results)

    # check that the inverse is also true.
    for row in all_results:
        _id = (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        if _id not in query_result_ids and start_key in row:
            assert row[start_key] > end or (end_key in row and row[end_key] <= start)

    # make sure that results are sorted by content_start
    for i in range(len(query_results) - 1):
        assert query_results[i][start_key] <= query_results[i + 1][start_key]

    # do a reverse query, everything should be the same except for the ordering
    query_results_2 = list(warehouse.query_metadata_items(query_range, ascending=False))
    query_result_ids_2 = set(
        (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        for row in query_results_2
    )
    assert len(query_results_2) == len(query_results)
    assert query_result_ids_2 == query_result_ids
    # assert backwards
    for i in range(len(query_results_2) - 1):
        assert query_results_2[i][start_key] >= query_results_2[i + 1][start_key]

    # filter for desired fields
    fields = ["url", "retrieved_date"]
    query_results_3 = list(warehouse.query_metadata_items(query_range, fields=fields))
    assert len(query_results_3) == len(query_results)
    assert all(not row.keys() - set(fields) for row in query_results_3)

    # empty query, no files exist in this range
    start = datetime(2010, 1, 1, tzinfo=timezone.utc)
    end = datetime(2015, 1, 1, tzinfo=timezone.utc)
    query_range = DatetimeRange(start, end)
    results = list(warehouse.query_metadata_items(query_range, ascending=False))
    assert len(results) == 0


def _test_query_between_range(warehouse, index, date_key):
    # stores 300 daily interval files from 2020-01-01 to 2020-01-11
    file_ids = set(_store_random_files_2020(warehouse, timedelta(days=1)))

    all_results = list(warehouse.query_metadata_items(index=index))
    # All generated files have a release
    if index == API.INDEXES.RELEASE:
        assert len(all_results) == len(file_ids)
    # Half of generated files have a content_start
    elif index == API.INDEXES.START:
        assert len(all_results) == len(file_ids) / 2

    # query for a specific range
    start = datetime(2020, 3, 3, tzinfo=timezone.utc)
    end = datetime(2020, 9, 6, tzinfo=timezone.utc)
    query_range = DatetimeRange(start, end)
    query_results = list(warehouse.query_metadata_items(query_range, index=index))
    query_result_ids = set(
        (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        for row in query_results
    )
    assert 0 < len(query_results) < len(file_ids)
    assert query_result_ids.issubset(file_ids)

    # check that the queried content is accurate.
    for row in query_results:
        assert start <= row[date_key] <= end

    # check that the inverse is also true.
    for row in all_results:
        _id = (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        if _id not in query_result_ids:
            assert row[date_key] < start or row[date_key] > end

    # make sure that results are sorted correctly
    for i in range(len(query_results) - 1):
        assert query_results[i][date_key] <= query_results[i + 1][date_key]

    # do a reverse query, everything should be the same except for the ordering
    query_results_2 = list(
        warehouse.query_metadata_items(query_range, index=index, ascending=False)
    )
    print(query_results_2[0])
    query_result_ids_2 = set(
        (warehouse.get_primary_key(row), warehouse.get_source_version(row))
        for row in query_results_2
    )
    assert len(query_results_2) == len(query_results)
    assert query_result_ids_2 == query_result_ids
    # assert backwards
    for i in range(len(query_results_2) - 1):
        assert query_results_2[i][date_key] >= query_results_2[i + 1][date_key]

    # filter for desired fields
    fields = ["url", "retrieved_date"]
    query_results_3 = list(
        warehouse.query_metadata_items(query_range, index=index, fields=fields)
    )
    assert len(query_results_3) == len(query_results)
    assert all(not row.keys() - set(fields) for row in query_results_3)

    # empty query, no files exist in this range
    start = datetime(2010, 1, 1, tzinfo=timezone.utc)
    end = datetime(2015, 1, 1, tzinfo=timezone.utc)
    query_range = DatetimeRange(start, end)
    results = list(
        warehouse.query_metadata_items(query_range, index=index, ascending=False)
    )
    assert len(results) == 0


def test_query_release(warehouse):
    _test_query_between_range(warehouse, API.INDEXES.RELEASE, release_key)


def test_query_start(warehouse):
    _test_query_between_range(warehouse, API.INDEXES.START, start_key)


def test_update_item(warehouse):
    warehouse.select_collection("test_query", database="test_database")

    # create a file
    dt = datetime(2020, 1, 2, 3, tzinfo=timezone.utc)
    metadata = dict(url="content-url", retrieved_date=dt, release_date=dt, key1="name")
    file = SeekableStream("some-content", **metadata)

    # store the file
    response = warehouse.store(file, force_store=True)
    assert response["status_code"] == warehouse.STATUS.SUCCESS
    file_key = response["primary_key"]
    file_version = response[API.VERSION_FIELD]

    # update the metadata
    new_dt = datetime(3030, 3, 2, 1, tzinfo=timezone.utc)
    update_map = {
        release_key: new_dt,  # replace old value
        start_key: new_dt,  # add new field
        end_key: new_dt,  # add new field
        "key1": "new_name",  # replace old value
    }
    warehouse.update_metadata_item(file_key, file_version, update_map)

    # Updating non existent item throws an exception
    with pytest.raises(exceptions.OperationError):
        warehouse.update_metadata_item(file_key, file_version[:-1], update_map)

    # retrieve metadata and verify the update
    stored = warehouse.retrieve(file_key, file_version, metadata_only=True)
    for key, val in update_map.items():
        assert stored[key] == val

    # Updating retrieved_date is not allowed
    update_map = {
        API.RETRIEVED_FIELD: new_dt,
    }
    with pytest.raises(exceptions.MetadataError):
        warehouse.update_metadata_item(file_key, file_version, update_map)

    # Updating primary key is not allowed
    update_map = {
        "url": "new_url",
    }
    with pytest.raises(exceptions.MetadataError):
        warehouse.update_metadata_item(file_key, file_version, update_map)
