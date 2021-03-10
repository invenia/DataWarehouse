import copy
import enum
import json
import logging
import os
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
    overload,
)

import boto3
import pytz
from datafeedscommon.aws.sts import assume_iam_role
from inveniautils.configuration import Configuration
from inveniautils.datetime_range import DatetimeRange
from inveniautils.stream import SeekableStream
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_s3.client import S3Client

from datawarehouse.exceptions import ArgumentError, MetadataError, OperationError
from datawarehouse.interface import DataWarehouseInterface as API
from datawarehouse.types import (
    TYPES,
    AllowedTypes,
    Encoded,
    TzTypes,
    ValueTypes,
    decode,
    encode,
)


LOGGER = logging.getLogger(__name__)

CONFIG_PATH_VAR = "WAREHOUSE_CONFIG_FILE"
CONFIG_PATH_DEFAULT = "settings.yaml"
CONFIG_PREFIX = "S3Warehouse"


# An encoded dynamodb item
DynamoClientItem = Mapping[str, Mapping[Literal["S", "N"], str]]


class S3Warehouse(API):
    """
    The Datafeeds S3 warehouse implementation. Implements the DataWarehouseInterface

    AWS resources used:
        - x1 S3 bucket for storing source files.
        - x1 S3 bucket for storing parsed files.
        - x1 DynamoDB table for as the warehouse registry.
        - x1 DynamoDB table for indexing source files.
    """

    # defaults
    DEFAULT_CACHE_TTL = 300
    DEFAULT_SESH_DURATION = 3600

    # The DynamoDB registry Table columns
    class REG(enum.Enum):
        ID = "feed_id"  # the hash key
        DB = "database"
        COL = "collection"
        PKEYS = "primary_key_fields"
        RKEYS = "required_metadata_fields"
        TMAP = "metadata_type_map"
        PARSERS = "parsers"

    # parser info fields
    class PAR(enum.Enum):
        PKEYS = "primary_key_fields"
        TMAP = "row_type_map"
        TZ = "timezone"
        DEFAULT = "default"

    def __init__(
        self,
        config: Optional[Configuration] = None,
        region_name: Optional[str] = None,
        registry_table_name: Optional[str] = None,
        source_table_name: Optional[str] = None,
        source_bucket_name: Optional[str] = None,
        parsed_bucket_name: Optional[str] = None,
        role_arn: Optional[str] = None,
        sesh_duration: Optional[int] = None,
        cache_ttl: Optional[int] = None,
        load_defaults: Optional[bool] = True,
        database: Optional[str] = None,
        collection: Optional[str] = None,
    ):
        """
        Instantiates the S3Warehouse.

        Args:
            config: A Configuration object that contains warehouse init args. If not
                provided, configs will be loaded from the DEFAULT_CONFIG_PATH. Configs
                can be overwritten by specifying addition kw args at init.
            region_name: The AWS region name that contains the warehouse resources.
            registry_table_name: The registry DynamoDB table.
            source_table_name: The source data DynamoDB table.
            source_bucket_name: The source data S3 Bucket.
            parsed_bucket_name: The parsed data S3 Bucket.
            role_arn: The role arn used if accessing the AWS resources via an IAM role.
            sesh_duration: If a role_arn is supplied, the session duration used for each
                call to sts-assume-role.
            cache_ttl: Registry cache TTL in seconds, can be set to 0. Defaults to 300s.
            load_defaults: Whether or not to load the default config file if no config
                object is passed in.
            database: Pre-selects a database, can be changed later on.
            collection: Pre-selects a collection, can be changed later on.
        """
        # Set default configs
        if config is None and load_defaults:
            default_path = os.environ.get(CONFIG_PATH_VAR, CONFIG_PATH_DEFAULT)
            config = Configuration(default_file_path=default_path)
        self._config = config if config else Configuration()

        # argument selector and checker
        def pick_arg(var, config_key, nullable=False):
            file_arg = self._config.get_constant([CONFIG_PREFIX, config_key])
            selected = var if var is not None else file_arg
            if selected is not None or nullable:
                return selected
            raise ArgumentError(
                f"Required arg '{config_key}' is neither available as a kwarg nor "
                "present in the provided/loaded configs."
            )

        # Set reqired args
        self._region: str = pick_arg(region_name, "region_name")
        self._registry_table: str = pick_arg(registry_table_name, "registry_table_name")
        self._source_table: str = pick_arg(source_table_name, "source_table_name")
        self._source_bucket: str = pick_arg(source_bucket_name, "source_bucket_name")
        self._parsed_bucket: str = pick_arg(parsed_bucket_name, "parsed_bucket_name")

        # Set optional aws role and sesh duration
        self._role_arn: Optional[str] = pick_arg(role_arn, "role_arn", nullable=True)
        self._sesh_duration: Optional[int] = pick_arg(
            sesh_duration, "sesh_duration", nullable=True
        )
        if self._role_arn and self._sesh_duration is None:
            self._sesh_duration = self.DEFAULT_SESH_DURATION

        # aws sesh and clients
        self._s3: Optional[S3Client] = None
        self._dynamo: Optional[DynamoDBClient] = None
        self._sesh: Optional[boto3.session.Session] = None
        self._sesh_expiry: Optional[datetime] = None

        # registry table cache
        cache_ttl = pick_arg(cache_ttl, "cache_ttl", nullable=True)
        cache_ttl = cache_ttl if cache_ttl is not None else self.DEFAULT_CACHE_TTL
        self._cache_ttl: timedelta = timedelta(seconds=cache_ttl)
        self._reg_cache: Dict[str, Tuple[Dict, datetime]] = {}
        self._last_scan: Optional[datetime] = None

        self._database: Optional[str] = None
        self._collection: Optional[str] = None

        # pre-select a database and collection if specified.
        if database and collection:
            self.select_collection(collection, database=database)
            self._database = database
            self._collection = collection

    def update_source_registry(
        self,
        database: str,
        collection: str,
        primary_key_fields: Optional[Union[str, Tuple[str, ...]]] = None,
        required_metadata_fields: Optional[Union[str, Iterable[str]]] = None,
        metadata_type_map: Optional[Dict[str, AllowedTypes]] = None,
    ):
        """
        Registers or updates a source collection in the data warehouse.

        Args:
            database: The name of the database to register or update.
            collection: The name of the collection to register or update.
            primary_key_fields: The primary key fields for source files in the
                collection. This is required when registering a new collection for the
                first time. Updates to an existing collection is not allowed.
            required_metadata_fields: Additional required source file metadata fields
                that are not primary key fields. Updates to an existing collection will
                completely replace the previous list.
            metadata_type_map: The type map for metadata fields. Types for primary key
                fields and required metadata fields must be defined. Updates to existing
                collections will only append to or correct the existing type map.

        Raises:
            ArgumentError: If there are any invalid combinations of arguments.
        """
        if isinstance(primary_key_fields, str):
            primary_key_fields = (primary_key_fields,)
        if isinstance(required_metadata_fields, str):
            required_metadata_fields = (required_metadata_fields,)

        try:
            entry = self._get_registry_entry(database, collection, use_cached=False)
        except OperationError:
            # This block means we are registering a new collection.
            if primary_key_fields is None or metadata_type_map is None:
                raise ArgumentError(
                    "'primary_key_fields' and 'metadata_type_map' are required when "
                    "registering a new collection."
                )
            entry = {
                self.REG.ID.value: self._get_collection_id(database, collection),
                self.REG.DB.value: database,
                self.REG.COL.value: collection,
                self.REG.PKEYS.value: primary_key_fields,
                self.REG.TMAP.value: metadata_type_map,
                self.REG.RKEYS.value: (),
                self.REG.PARSERS.value: {},
            }
        else:
            # This block means the collection already exist.
            if primary_key_fields and primary_key_fields != entry[self.REG.PKEYS.value]:
                raise ArgumentError(
                    "Updating the primary key fields of a collection is not allowed."
                )
            elif metadata_type_map is not None:
                entry[self.REG.TMAP.value].update(metadata_type_map)

        # Updates to an existing collection will completely replace the previous list.
        if required_metadata_fields is not None:
            entry[self.REG.RKEYS.value] = required_metadata_fields

        # ensure that type maps exist for all required keys.
        all_required = set(entry[self.REG.PKEYS.value] + entry[self.REG.RKEYS.value])
        missing = all_required - entry[self.REG.TMAP.value].keys()
        if missing:
            raise ArgumentError(f"The type map is missing keys for {missing}.")

        # Push changes to dynamo then update local cache.
        # boto3-stubs[dynamodb] uses a very loose but non-flexible type def, strip
        # away our DynamoClientItem type so mypy doesn't compain.
        encoded = cast(Any, self._encode_registry(entry))
        self._dynamo_client.put_item(TableName=self._registry_table, Item=encoded)
        self._update_cache(entry)

    def update_parsed_registry(
        self,
        database: str,
        collection: str,
        parser_name: str,
        primary_key_fields: Optional[Union[str, Tuple[str, ...]]] = None,
        row_type_map: Optional[Dict[str, AllowedTypes]] = None,
        timezone: Optional[TzTypes] = None,
        promote_default: bool = False,
    ):
        """
        Registers or updates a parsed collection in the data warehouse.

        Args:
            database: The name of the database to register or update.
            collection: The name of the collection to register or update.
            parser_name: The name of the parser.
            primary_key_fields: The primary key columns for parsed rows. This is
                required when registering a new parser for the first time. Updates to an
                existing parser will completely replace the existing keys.
            row_type_map: The type map for parsed rows. This is required when
                registering a new parser for the first time. Updates to an existing
                parser will completely replace the existing type map.
            timezone: The timezone used by the parser. This is required when registering
                a new parser for the first time. Updates to an existing parser will
                completely replace the existing timezone.
            promote_default: Whether to promote the new or existing parser as the
                default. The first parser of a collection always starts as the default.

        Raises:
            ArgumentError: If there are any invalid combinations of arguments.
        """
        if isinstance(primary_key_fields, str):
            primary_key_fields = (primary_key_fields,)

        # Get entry from dynamo, errors if the database/collection doesn't exist.
        entry = self._get_registry_entry(database, collection, use_cached=False)

        parsers = entry[self.REG.PARSERS.value]
        if parser_name not in parsers:
            # registering a new parser
            if any(arg is None for arg in (primary_key_fields, row_type_map, timezone)):
                raise ArgumentError(
                    "p_keys, type_map, and timezone must be specified when registering "
                    "a parser for the first time."
                )
            parser = {}
        else:
            # updating an existing parser
            parser = parsers[parser_name]

        # update all info.
        if primary_key_fields:
            parser[self.PAR.PKEYS.value] = primary_key_fields
        if row_type_map:
            parser[self.PAR.TMAP.value] = row_type_map
        if timezone:
            parser[self.PAR.TZ.value] = timezone

        # ensure that a type map exist for all primary keys.
        missing = set(parser[self.PAR.PKEYS.value]) - parser[self.PAR.TMAP.value].keys()
        if missing:
            raise ArgumentError(f"The type map is missing keys for {missing}.")

        parsers[parser_name] = parser

        # if only 1 parser is available, set it as the default regardless.
        if len(parsers) == 1 or promote_default:
            parsers[parser_name][self.PAR.DEFAULT.value] = True
            for parser in parsers:
                if parser != parser_name:
                    # set all other parsers as non-default
                    parsers[parser][self.PAR.DEFAULT.value] = False

        # push changes to dynamo then update local cache
        entry[self.REG.PARSERS.value] = parsers
        # boto3-stubs[dynamodb] uses a very loose but non-flexible type def, strip
        # away our DynamoClientItem type so mypy doesn't compain.
        encoded = cast(Any, self._encode_registry(entry))
        self._dynamo_client.put_item(TableName=self._registry_table, Item=encoded)
        self._update_cache(entry)

    def list_databases_and_collections(self) -> Dict[str, List[str]]:
        """ Lists all registered databases and collections in the data warehouse. """
        results: Dict[str, List[str]] = {}
        for entry in self._iter_registry():
            db = entry[self.REG.DB.value]
            coll = entry[self.REG.COL.value]
            if db not in results:
                results[db] = []
            results[db].append(coll)
        for db in results:
            results[db].sort()
        return results

    def list_databases(self) -> List[str]:
        """ Lists all registered databases in the data warehouse. """
        return sorted(self.list_databases_and_collections().keys())

    def list_collections(self) -> List[str]:
        """ Lists all registered collections in the current database. """
        return sorted(self.list_databases_and_collections()[self.database])

    def select_collection(self, collection: str, database: Optional[str] = None):
        """Selects a database and collection.

        Args:
            database: The name of the database that the collection belongs to.
            collection: The name of the collection to select.

        Raises:
            OperationError: If the collection and/or databsae doesn't exist.
        """
        database = database if database else self.database
        try:
            reg_entry = self._get_registry_entry(database, collection, use_cached=False)
        except OperationError:
            raise OperationError(
                f"Unable to select {database} - {collection}, combo does not exist."
            )
        else:
            self._database = reg_entry[self.REG.DB.value]
            self._collection = reg_entry[self.REG.COL.value]

    @property
    def database(self) -> str:
        """ The currently selected database. """
        if self._database is None:
            raise OperationError("No database selected.")
        return self._database

    @property
    def collection(self) -> str:
        """ The currently selected collection. """
        if self._collection is None:
            raise OperationError("No collection selected.")
        return self._collection

    @property
    def primary_key_fields(self) -> Tuple[str, ...]:
        """The primary key fields for files in the collection

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        return entry[self.REG.PKEYS.value]

    @property
    def required_metadata_fields(self) -> Tuple[str, ...]:
        """All required metadata fields including primary keys.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        p_keys = list(entry[self.REG.PKEYS.value])
        # add the other required keys to the set while maitaining order
        for k in entry[self.REG.RKEYS.value]:
            if k not in p_keys:
                p_keys.append(k)
        return tuple(p_keys)

    @property
    def metadata_type_map(self) -> Dict[str, AllowedTypes]:
        """The metadata type map for files in the collection.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        return entry[self.REG.TMAP.value]

    @property
    def default_parser_name(self) -> str:
        """The collection's default parser name.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers registered with the collection.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        parsers = entry[self.REG.PARSERS.value]
        if len(parsers) == 0:
            raise OperationError(
                f"There are no parsers for the {self.database}, {self.collection}."
            )
        for parser, info in parsers.items():
            if info[self.PAR.DEFAULT.value]:
                return parser
        raise ValueError(
            f"None of the parsers in {self.database}, {self.collection} are default."
        )

    @property
    def default_parser_pkey_fields(self) -> Tuple[str, ...]:
        """The collection's default parser's primary keys.

        Raises:
             OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        default = self.default_parser_name
        return entry[self.REG.PARSERS.value][default][self.PAR.PKEYS.value]

    @property
    def default_parser_type_map(self) -> Dict[str, AllowedTypes]:
        """The collection's default parser's type map.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        default = self.default_parser_name
        return entry[self.REG.PARSERS.value][default][self.PAR.TMAP.value]

    @property
    def default_parser_timezone(self) -> TzTypes:
        """The collection's default parser's timezone.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        default = self.default_parser_name
        return entry[self.REG.PARSERS.value][default][self.PAR.TZ.value]

    @property
    def available_parsers(self) -> Dict[str, Dict[str, Any]]:
        """
        Type maps, time zones, and other information for all available parsers
        that are associated with the collection.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        entry = self._get_registry_entry(self.database, self.collection)
        return entry[self.REG.PARSERS.value]

    def get_primary_key(
        self, metadata: Dict[str, ValueTypes]
    ) -> Tuple[ValueTypes, ...]:
        """Extracts primary key values from a file's metadata entry.

        Raises:
            OperationError: If no database and/or collection is selected.
            MetadataError: If any primary key fields are missing from metadata.
        """
        keys = self.primary_key_fields
        missing = set(keys) - metadata.keys()
        if missing:
            raise MetadataError(f"Primary key fields {missing} are missing.")
        return tuple(metadata[k] for k in keys)

    def get_source_version(self, metadata: Dict[str, ValueTypes]) -> str:
        """Extracts the source file version from a file's metadata entry.

        Raises:
            MetadataError: If the VERSION_FIELD is missing from metadata.
        """
        version = metadata.get(self.VERSION_FIELD)
        if version is None:
            raise MetadataError(f"The version field {self.VERSION_FIELD} is missing.")
        elif not isinstance(version, str):
            raise MetadataError(f"Invalid version type {type(version)}, expected str.")
        else:
            return version

    def store(
        self,
        file: SeekableStream,
        parsed_file: bool = False,
        force_store: bool = False,
        compare: Optional[Callable[[SeekableStream, SeekableStream], bool]] = None,
        parser_name: Optional[str] = None,
    ) -> Dict[str, Union[Tuple[ValueTypes, ...], str, API.STATUS]]:
        """
        Stores either a source file or a parsed file.

        If storing a source file:
          - Using force_store will always store a source file as a new version.
          - A source version id is always automatically generated when storing a source
            file.
          - If not force_store, the new source file will always be compared against the
            previously stored file with the same primary key (if one exists) to check if
            it has been updated. The new source file will only be stored if it has been
            updated. This comparison can be done in 2 ways:
              - By simply comparing the LAST_MODIFIED_FIELD if it is listed in the
                "required_metadata_fields" of the collection.
              - By actually comparing both files (if LAST_MODIFIED_FIELD is not listed
                in the "required_metadata_fields"). If no compare method is supplied by
                the caller, a direct comparison of the file objects' contents are done.
          - If the type map for a metadata field is not registered in the source file
            registry table, it will be encoded as a string.

        If storing a parsed file:
          - The VERSION_FIELD must be available in the file's metadata and it must
            correspond to an existing sources file version.
          - If no parser_name is specified, the default parser is assumed.
          - Always overwrites the previous parsed file if one already exist for the
            given parser and source version.

        Args:
            file: The file to store.
            parsed_file: Whether the file is a source file or a parsed file. Defaults to
                a source file.
            force_store: Whether to force store a source file as a new version without
                duplication checks. Only relevant to source files.
            compare: The compare function to compare SeekableStream objects
                before storing.
            parser_name: The name of the parser, only relevant when storing parsed file.
                Assumes the default parser when not specified.

        Returns:
            A dict of the status code, primary key, and source version id of the file.
            If the status code is STATUS.ALREADY_EXIST, the primary key and version id
            will be in reference to the already stored file.
            Optionally returns the parser name if storing a parsed file.
            {
                "primary_key": Tuple[ValueTypes, ...],
                "source_version": str,
                "status_code": STATUS
                "parser_name": str, (optional)
            }

        Raises:
            OperationError: If no database and/or collection is selected or if
                trying to store a parsed file with an unregistered parser.
            ArgumentError: If there are any invalid combinations of arguments.
            MetadataError: If there are any problems with the file metadata.
        """
        raise NotImplementedError

    def retrieve_versions(
        self,
        primary_key: Union[ValueTypes, Tuple[ValueTypes, ...]],
        metadata_only: bool = False,
        parsed_file: bool = False,
        parser_name: Optional[str] = None,
        latest_first: bool = True,
    ) -> Union[
        Generator[SeekableStream, None, None],
        Generator[Dict[str, ValueTypes], None, None],
    ]:
        """
        Retrieves all versions of a target file, with the option of retrieving just the
        file metadata. If retrieving a parsed file, the default parser will be used
        unless specified otherwise.

        Args:
            primary_key: Primary key values of the file.
            metadata_only: Only return metadata, not file contents.
            parsed_file: Retrieves the parsed file instead of the source file.
            parser_name: Name of the parser, assumes default when not specified.
            latest_first: Ordering of results by retrieval date.

        Yields:
            The lazilly-loaded SeekableStream file object(s) or metadata item(s).

        Raises:
            OperationError: If no database and/or collection is selected or if
                trying to retrieve parsed files for an invalid parser.
            ArgumentError: If there are any invalid combinations of arguments.
        """
        raise NotImplementedError

    def retrieve(
        self,
        primary_key: Union[ValueTypes, Tuple[ValueTypes, ...]],
        source_version: Optional[str] = None,
        metadata_only: bool = False,
        parsed_file: bool = False,
        parser_name: Optional[str] = None,
    ) -> Optional[Union[SeekableStream, Dict[str, ValueTypes]]]:
        """
        Retrieves a target file, with the option of retrieving just the metadata.
        Defaults to the latest retrieved version if version is not specified. Also has
        the option to retrieve parsed files instead of source files.

        Args:
            primary_key: Primary key values of the file.
            source_version: The source file version, defaults to latest.
            metadata_only: Only return metadata, not file contents.
            parsed_file: Retrieves the parsed file instead of the source file
            parser_name: Name of the parser, assumes default when not specified.

        Returns:
            SeekableStream file object or metadata item, or None if no such file exist.

        Raises:
            OperationError: If no database and/or collection is selected or if
                trying to retrieve parsed files for an invalid parser.
            ArgumentError: If there are any invalid combinations of arguments.
        """
        raise NotImplementedError

    def delete(
        self,
        primary_key: Union[ValueTypes, Tuple[ValueTypes, ...]],
        source_version: Optional[str] = None,
        parsed_files_only: bool = False,
        parser_name: str = "all",
    ) -> Optional[Iterable[Callable[[], None]]]:
        """
        Deletes a target file from the warehouse.

        General behaviour:
        - If a source_version is specified or if a source_version is not
          specified but only one version is available, delete the version.
        - When a source version is deleted, all associated parsed files are
          deleted as well.
        - Has the option to only delete parsed files for the source_version.
        - If parser name is not specified when deleting parsed files,
          deletes parsed files for all parsers.

        Special behaviour:
        - If a source_version is not specified and multiple versions are found,
          instead of deleting all versions right away, returns an iterable of
          callables to delete each version.
          eg.
            > callables = wh.delete(primary_key) # 5 versions found
              Warning: 5 versions found for key A, holding off delete.
            > print(len(callables))
              5
            > print(callables[0].metadata)  # version1’s metadata, for inspection.
              {"k1": "v1", "k2": "v2", ...}
            > callables[0]()  # a call to delete version 1
              Info: Deleting version 1 for key A...
            > print(callables[1].metadata)  # version2’s metadata, for inspection.
              {"k1": "v1", "k2": "v2", ...}
            > callables[1]()  # a call to delete version 2
              Info: Deleting version 2 for key A...

        Args:
            primary_key: Primary key values of the file.
            source_version: Source file version, deletes all versions if None.
            parsed_files_only: Only deletes parsed files.
            parser_name: Name of the parser, defaults to all parsers.

        Retruns:
            None or an iterable or callables to deleted individual file versions found.

        Raises:
            OperationError: If no database and/or collection is selected or if
                trying to delete non-existent files.
        """
        raise NotImplementedError

    def query_metadata_items(
        self,
        query_range: Optional[DatetimeRange] = None,
        index: API.INDEXES = API.INDEXES.CONTENT,
        fields: Optional[Iterable[str]] = None,
        ascending: bool = True,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Finds all metadata items that overlap with the given query_range.
        The query_range will correspond to the index being used.
        All metadata items are returned if no range is specified.

        Args:
            query_range: The overlapping DatetimeRange to query for.
                Supports bounds checking and +/- Inf bounds.
            index: The index to use, of type (Enum) DataWarehouseInterface.INDEX
            fields: If provided, returns only the specified metadata fields.
            ascending: Ordering of results, correspond to the index being used.

        Yields:
            The lazilly-loaded file metadata item(s).

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        raise NotImplementedError

    def update_metadata_item(
        self,
        primary_key: Union[ValueTypes, Tuple[ValueTypes, ...]],
        source_version: str,
        update_map: Dict[str, ValueTypes],
    ):
        """
        Updates the metadata for an existing source file entry. Raises an exception for
        any failures.

        Args:
            primary_key: Source file primary key.
            source_version: Source file version.
            update_map: Metadata key-val pairs to add/update.

        Raises:
            OperationError: If no database and/or collection is selected or if
                no such file with the given primary key and version id exists.
            MetadataError: If there are any issues with the metadata update map.
        """
        raise NotImplementedError

    def export(
        self,
        database: str,
        collection: Optional[str] = None,
        **dest_args,
    ):
        """
        Exports source files from a database or collection to an S3 bucket or
        download locally.

        Args:
            database: The database to export.
            collection: The collection to export. Exports all collections if None.
            dest_args: Implementation specific destination kwargs.
        """
        raise NotImplementedError

    @staticmethod
    def migrate(
        source: API,
        dest: API,
        database: Optional[str] = None,
        collection: Optional[str] = None,
    ):
        """
        Migrates databases or collections from one warehouse to another.

        Args:
            source: Source DataWarehouseInterface instance.
            dest: Destination DataWarehouseInterface instance.
            database: Name of the database.
            collection: Name of the collection.
        """
        raise NotImplementedError

    @property
    def _s3_client(self) -> S3Client:
        self._s3 = self._get_client("s3")
        return self._s3

    @property
    def _dynamo_client(self) -> DynamoDBClient:
        self._dynamo = self._get_client("dynamodb")
        return self._dynamo

    @overload
    def _get_client(self, name: Literal["dynamodb"]) -> DynamoDBClient:
        ...

    @overload
    def _get_client(self, name: Literal["s3"]) -> S3Client:
        ...

    def _get_client(
        self, name: Literal["dynamodb", "s3"]
    ) -> Union[DynamoDBClient, S3Client]:
        client = self._s3 if name == "s3" else self._dynamo
        renew = self._sesh_expiry is None or datetime.now(pytz.utc) >= self._sesh_expiry
        # if a role arn is passed in, use to role credentials.
        if self._role_arn and (renew or client is None):
            if renew:
                self._renew_sesh()
            self._sesh = cast(boto3.session.Session, self._sesh)  # or mypy complains
            client = self._sesh.client(name, region_name=self._region)
        # uses default credentials
        elif client is None:
            client = boto3.client(name, region_name=self._region)

        return client

    def _renew_sesh(self):
        sesh, expiry = assume_iam_role(
            role_arn=self._role_arn,
            sesh_name=self.__class__.__name__,
            sesh_duration=self._sesh_duration,
        )
        # such that we will renew the session 1 mins before it actually expires.
        self._sesh_expiry = expiry - timedelta(minutes=1)
        self._sesh = sesh
        self._s3 = None
        self._dynamo = None

    def _get_collection_id(self, db: str, coll: str) -> str:
        return "_".join([db, coll])

    def _update_cache(self, row: Dict[str, Any]):
        """ Update the cache with a new entry. """
        _id = row[self.REG.ID.value]
        self._reg_cache[_id] = (copy.deepcopy(row), datetime.now(pytz.utc))

    def _get_cached(self, db: str, coll: str) -> Optional[Dict[str, Any]]:
        """ Gets a cached entry. Returns None if one doesn't exist or has expired. """
        _id = self._get_collection_id(db, coll)
        if _id in self._reg_cache:
            row, time = self._reg_cache[_id]
            if datetime.now(pytz.utc) < time + self._cache_ttl:
                return copy.deepcopy(row)
            del self._reg_cache[_id]
        return None

    def _iter_registry(self, use_cached: bool = True) -> Iterator[Dict[str, Any]]:
        """Iterates over all registry entries. Has the option to use cached entries
        instead if a table scan was previously done and the cache hasn't expired.
        """
        now = datetime.now(pytz.utc)
        scan_still_valid = self._last_scan and now < self._last_scan + self._cache_ttl
        if use_cached and scan_still_valid:
            for _id in sorted(self._reg_cache.keys()):
                row, time = self._reg_cache[_id]
                yield copy.deepcopy(row)
        else:
            table_scan = self._dynamo_client.get_paginator("scan")
            for page in table_scan.paginate(TableName=self._registry_table):
                for item in page["Items"]:
                    decoded = self._decode_registry(cast(DynamoClientItem, item))
                    self._update_cache(decoded)
                    yield decoded
            self._last_scan = now

    def _get_registry_entry(
        self, db: str, coll: str, use_cached: bool = True
    ) -> Dict[str, Any]:
        """Retrieves and caches the registry entry for a collection. Has the option
        to use cached entries instead if one available and the cache hasn't expired.
        """
        if use_cached:
            cached = self._get_cached(db, coll)
            if cached:
                return cached

        _id = self._get_collection_id(db, coll)
        resp = self._dynamo_client.get_item(
            TableName=self._registry_table,
            Key={self.REG.ID.value: {"S": _id}},
        )
        if "Item" in resp:
            decoded = self._decode_registry(cast(DynamoClientItem, resp["Item"]))
            self._update_cache(decoded)
            return decoded
        else:
            raise OperationError(f"Invalid collection '{coll}' and database '{db}'")

    def _encode_registry(self, entry: Dict[str, Any]) -> DynamoClientItem:
        """ Encodes a registry to prep it for storing in DDB. """
        keys_enc = lambda val: json.dumps(list(val))
        map_enc = lambda val: json.dumps({k: TYPES(v).name for k, v in val.items()})
        type_enc = lambda val: encode(val).serialize()

        parser_enc: Dict[S3Warehouse.PAR, Callable[[Any], str]] = {
            self.PAR.PKEYS: keys_enc,
            self.PAR.TMAP: map_enc,
            self.PAR.TZ: type_enc,
            self.PAR.DEFAULT: type_enc,
        }
        registry_enc: Dict[S3Warehouse.REG, Callable[[Any], str]] = {
            self.REG.ID: str,
            self.REG.DB: str,
            self.REG.COL: str,
            self.REG.PKEYS: keys_enc,
            self.REG.RKEYS: keys_enc,
            self.REG.TMAP: map_enc,
            self.REG.PARSERS: lambda val: json.dumps(
                {
                    parser_name: {
                        k: parser_enc[self.PAR(k)](v) for k, v in parser_entry.items()
                    }
                    for parser_name, parser_entry in val.items()
                }
            ),
        }

        strings: Dict[str, str]
        strings = {k: registry_enc[self.REG(k)](v) for k, v in entry.items()}

        return {k: {"S": v} for k, v in strings.items()}

    def _decode_registry(self, entry: DynamoClientItem) -> Dict[str, Any]:
        """ Decodes a registry entry from DDB. """
        keys_dec = lambda val: tuple(json.loads(val))
        map_dec = lambda val: {k: TYPES[v].value for k, v in json.loads(val).items()}
        type_dec = lambda val: decode(Encoded.deserialize(val))

        parser_dec: Dict[S3Warehouse.PAR, Callable[[str], Any]] = {
            self.PAR.PKEYS: keys_dec,
            self.PAR.TMAP: map_dec,
            self.PAR.TZ: type_dec,
            self.PAR.DEFAULT: type_dec,
        }
        registry_dec: Dict[S3Warehouse.REG, Callable[[str], Any]] = {
            self.REG.ID: str,
            self.REG.DB: str,
            self.REG.COL: str,
            self.REG.PKEYS: keys_dec,
            self.REG.RKEYS: keys_dec,
            self.REG.TMAP: map_dec,
            self.REG.PARSERS: lambda val: {
                parser_name: {
                    k: parser_dec[self.PAR(k)](v) for k, v in parser_entry.items()
                }
                for parser_name, parser_entry in json.loads(val).items()
            },
        }

        sanitized: Dict[str, str]
        sanitized = {k: v["S"] for k, v in entry.items()}

        return {k: registry_dec[self.REG(k)](v) for k, v in sanitized.items()}
