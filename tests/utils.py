from datetime import datetime
from pathlib import Path

import yaml
from inveniautils.stream import SeekableStream

from datawarehouse.types import TYPES, Encoded, decode


fromiso = datetime.fromisoformat


def get_streams(database=None, collection=None, parsed=False):
    """ Loads in a bunch of SeekableStreams from a test file."""
    streams = []
    loaded = yaml.safe_load(Path("tests/files/warehouse_test_files.yaml").read_text())
    dt_fields = (
        "retrieved_date",
        "release_date",
        "content_start",
        "content_end",
        "content_resolution",
        "last-modified",
    )
    _file_type = "parsed" if parsed else "source"
    for file_type, dbs in loaded.items():
        if file_type != _file_type:
            continue
        for db, colls in dbs.items():
            if database and database != db:
                continue
            for coll, files in colls.items():
                if collection and collection != coll:
                    continue
                for file in files:
                    metadata = file["metadata"]
                    for field in dt_fields:
                        if field in metadata:
                            metadata[field] = fromiso(metadata[field])
                    content = file["content"]
                    if metadata.get("bytes"):
                        content = content.encode()
                    streams.append(SeekableStream(content, **metadata))
    return streams


# helper method to register some new collectons in the backend
def register_test_collections(warehouse):
    collections = load_test_collections_file()
    for db, colls in collections["source"].items():
        for coll, fields in colls.items():
            warehouse.update_source_registry(db, coll, **fields)
    for db, colls in collections["parsed"].items():
        for coll, parsers in colls.items():
            for parser in parsers:
                warehouse.update_parsed_registry(db, coll, **parser)


# helper method to load in some test collections from a file.
def load_test_collections_file():
    loaded = yaml.safe_load(Path("tests/files/warehouse_collection.yaml").read_text())
    for db, colls in loaded["source"].items():
        for coll, fields in colls.items():
            fields["primary_key_fields"] = tuple(fields["primary_key_fields"])
            fields["required_metadata_fields"] = tuple(
                fields["required_metadata_fields"]
            )
            for k, v in fields["metadata_type_map"].items():
                fields["metadata_type_map"][k] = TYPES[v].value
    for db, colls in loaded["parsed"].items():
        for coll, parsers in colls.items():
            for parser in parsers:
                parser["primary_key_fields"] = tuple(parser["primary_key_fields"])
                parser["timezone"] = decode(Encoded.deserialize(parser["timezone"]))
                for k, v in parser["row_type_map"].items():
                    parser["row_type_map"][k] = TYPES[v].value
                parser["row_type_map"][k]

    return loaded


def load_timestamps_2020():
    """ Loads in 300 randomly generated timestamps (in 2020) from a file. """
    return [
        int(line.strip())
        for line in Path("tests/files/random_ts_2020.txt").open().readlines()
    ]
