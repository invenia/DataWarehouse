import enum
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from inveniautils.datetime_range import DatetimeRange
from inveniautils.stream import SeekableStream

from datawarehouse.types import AllowedTypes, TzTypes, ValueTypes


class DataWarehouseInterface:
    """
    The Datafeeds warehouse interface for storing and retrieving source files and parsed
    files.

    General warehouse information:
      - The warehouse is divided into multiple databases (grids) with each database
        containing multiple collections (feeds).
      - Source files:
          - Source files in a collection are identified using a primary key, and each
            collection defines their own set of primary key fields.
          - Collections support storing multiple source files that have the same
            primary key, this is done by generating a unique version id for each file
            before storing.
      - Parsed files:
          - A collection can be registered with multiple parsers, and each source file
            version in a collection can be mapped to 1 parsed file per parser available.
          - Note that all parsed files must be linked to one and only one source file,
            and there can only be one parsed file per parser per source file version at
            any point in time.

    Constants:
        INDEXES: An Enum of avaiable indexes to query files by.
        STATUS: An Enum of status codes returned by certain warehouse operations.
        CONTENT_START_FIELD: Field name that parsed files should use to indicate the
            start date of contents in the file.
        LAST_MODIFIED_FIELD: Field name that source files should use to indicate its
            last modified date or publication date, if available.

            If this field is listed in the collection's "required_metadata_fields", it
            will be assumed that this field is a reliable indicator for if a new version
            of the file has been released.
        RETRIEVED_FIELD: Field name that source files should use for its retrieved date.
        RELEASE_FIELD: Field name that source files should use for its release date.
            The release date of a file should be equal to its last modified date. If
            the last modified date is not available, it should be the retrieved date.
        VERSION_FIELD: Field name used by the warehouse to assign version ids to stored
            source files.
    """

    class INDEXES(enum.Enum):
        CONTENT = enum.auto()
        RELEASE = enum.auto()

    class STATUS(enum.Enum):
        SUCCESS = enum.auto()
        ALREADY_EXIST = enum.auto()
        # FAILED = enum.auto() # not necessary because an exception will be raised.

    # public class constants
    COLLECTION_ID = "feed_id"
    CONTENT_START_FIELD = "content_start"
    CONTENT_END_FIELD = "content_end"
    CONTENT_RES_FIELD = "content_resolution"
    LAST_MODIFIED_FIELD = "last-modified"
    RETRIEVED_FIELD = "retrieved_date"
    RELEASE_FIELD = "release_date"
    VERSION_FIELD = "source_version"

    # private class constants
    _CONTENT_TYPE_FIELD = "content_type"

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
        raise NotImplementedError

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
        raise NotImplementedError

    def list_databases_and_collections(self) -> Dict[str, List[str]]:
        """ Lists all registered databases and collections in the data warehouse. """
        raise NotImplementedError

    def list_databases(self) -> List[str]:
        """ Lists all registered databases in the data warehouse. """
        raise NotImplementedError

    def list_collections(self) -> List[str]:
        """ Lists all registered collections in the current database. """
        raise NotImplementedError

    def select_collection(self, collection: str, *, database: Optional[str] = None):
        """Selects a collection (and database).

        Args:
            collection: The name of the collection to select.
            database: The name of the database that the collection belongs to. Defaults
                to the currently selected database.

        Raises:
            OperationError: If the collection and/or databsae doesn't exist.
        """
        raise NotImplementedError

    @property
    def database(self) -> str:
        """ The currently selected database. """
        raise NotImplementedError

    @property
    def collection(self) -> str:
        """ The currently selected collection. """
        raise NotImplementedError

    @property
    def primary_key_fields(self) -> Tuple[str, ...]:
        """The primary key fields for files in the collection

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        raise NotImplementedError

    @property
    def required_metadata_fields(self) -> Tuple[str, ...]:
        """All required metadata fields including primary keys.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        raise NotImplementedError

    @property
    def metadata_type_map(self) -> Dict[str, AllowedTypes]:
        """The metadata type map for files in the collection.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        raise NotImplementedError

    @property
    def default_parser_name(self) -> str:
        """The collection's default parser name.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        raise NotImplementedError

    @property
    def default_parser_pkey_fields(self) -> Tuple[str, ...]:
        """The collection's default parser's primary keys.

        Raises:
             OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        raise NotImplementedError

    @property
    def default_parser_type_map(self) -> Dict[str, AllowedTypes]:
        """The collection's default parser's type map.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        raise NotImplementedError

    @property
    def default_parser_timezone(self) -> TzTypes:
        """The collection's default parser's timezone.

        Raises:
            OperationError: If no database and/or collection is selected or if
                there are no parsers are registered with the collection.
        """
        raise NotImplementedError

    @property
    def available_parsers(self) -> Dict[str, Dict[str, Any]]:
        """
        Type maps, time zones, and other information for all available parsers
        that are associated with the collection.

        Raises:
            OperationError: If no database and/or collection is selected.
        """
        raise NotImplementedError

    def get_primary_key(
        self, metadata: Dict[str, ValueTypes]
    ) -> Tuple[ValueTypes, ...]:
        """Extracts primary key values from a file's metadata entry.

        Raises:
            OperationError: If no database and/or collection is selected.
            MetadataError: If any primary key fields are missing from metadata.
        """
        raise NotImplementedError

    def get_source_version(self, metadata: Dict[str, ValueTypes]) -> str:
        """Extracts the source file version from a file's metadata entry.

        Raises:
            MetadataError: If the VERSION_FIELD is missing from metadata.
        """
        raise NotImplementedError

    def store(
        self,
        file: SeekableStream,
        parsed_file: bool = False,
        force_store: bool = False,
        compare_source: Optional[
            Callable[[SeekableStream, SeekableStream], bool]
        ] = None,
        parser_name: Optional[str] = None,
    ) -> Dict[str, Union[Tuple[ValueTypes, ...], str, STATUS]]:
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
            compare_source: The compare function to compare SeekableStream objects
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
        index: Optional[INDEXES] = None,
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
        source: "DataWarehouseInterface",
        dest: "DataWarehouseInterface",
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
