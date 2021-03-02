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

from datawarehouse.types import TYPES, AllowedTypes, AllowedTZs


class DataWarehouseInterface:
    """
    The Datafeeds warehouse interface for storing and retrieving source files and parsed
    files.

    General information:
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
    """

    # Supported source file indexes when making queries.
    class INDEXES(enum.Enum):
        CONTENT = enum.auto()
        RELEASE = enum.auto()

    # If LAST_MODIFIED_FIELD is listed in "required_metadata_fields", it
    # will be assumed that this field is a reliable indicator for if a
    # new version of the file has been released.
    LAST_MODIFIED_FIELD = "last-modified"

    # These 2 fields are required when storing a source file.
    RETRIEVED_FIELD = "retrieved_date"
    RELEASE_FIELD = "release_date"

    # This field is required when storing a parsed file.
    CONTENT_START_FIELD = "content_start"

    # These fields are automatically added to every source file upon storing.
    VERSION_FIELD = "source_version"
    CONTENT_TYPE_FIELD = "content_type"

    def update_source_registry(
        self,
        database: str,
        collection: str,
        primary_key_fields: Optional[Union[str, Tuple[str, ...]]] = None,
        required_metadata_fields: Optional[Union[str, Iterable[str]]] = None,
        metadata_type_map: Optional[Dict[str, TYPES]] = None,
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
        """
        raise NotImplementedError

    def update_parsed_registry(
        self,
        database: str,
        collection: str,
        parser_name: str,
        primary_key_fields: Optional[Union[str, Tuple[str, ...]]] = None,
        row_type_map: Optional[Dict[str, TYPES]] = None,
        timezone: Optional[AllowedTZs] = None,
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
        """
        raise NotImplementedError

    def list_databases_and_collections(self) -> Dict[str, List[str]]:
        """
        Lists all registered databases and collections in the data warehouse.
        """
        raise NotImplementedError

    def list_databases(self) -> List[str]:
        """ Lists all registered databases in the data warehouse. """
        raise NotImplementedError

    def list_collections(self) -> List[str]:
        """ Lists all registered collections in the current database. """
        raise NotImplementedError

    def select_database(self, database: str):
        """ Selects a database without specifying any collection. """
        raise NotImplementedError

    def select_collection(self, collection: str, database: Optional[str] = None):
        """ Selects a collection (and database). """
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
        """ The primary key fields for files in the collection. """
        raise NotImplementedError

    @property
    def required_metadata_fields(self) -> Tuple[str, ...]:
        """ All required metadata fields including primary keys. """
        raise NotImplementedError

    @property
    def metadata_type_map(self) -> Dict[str, TYPES]:
        """ The metadata type map for files in the collection. """
        raise NotImplementedError

    @property
    def default_parser_name(self) -> str:
        """ The collection's default parser name. """
        raise NotImplementedError

    @property
    def default_parser_pkey_fields(self) -> Tuple[str, ...]:
        """ The collection's default parser's primary keys. """
        raise NotImplementedError

    @property
    def default_parser_type_map(self) -> Dict[str, TYPES]:
        """ The collection's default parser's type map. """
        raise NotImplementedError

    @property
    def default_parser_timezone(self) -> AllowedTZs:
        """ The collection's default parser's timezone. """
        raise NotImplementedError

    @property
    def available_parsers(self) -> Dict[str, Dict[str, Any]]:
        """
        Type maps, time zones, and other information for all available parsers
        that are associated with the collection.
        """
        raise NotImplementedError

    def get_primary_key(
        self, metadata: Dict[str, AllowedTypes]
    ) -> Tuple[AllowedTypes, ...]:
        """ Extracts primary key values from a file's metadata entry. """
        raise NotImplementedError

    def get_source_version(self, metadata: Dict[str, AllowedTypes]) -> str:
        """ Extracts the source file version from a file's metadata entry. """
        raise NotImplementedError

    def store(
        self,
        file: SeekableStream,
        parsed_file: bool = False,
        force_store: bool = False,
        compare: Optional[Callable[[SeekableStream, SeekableStream], bool]] = None,
        source_version: Optional[str] = None,
        parser_name: Optional[str] = None,
    ) -> Dict[str, Union[str, Tuple[AllowedTypes, ...]]]:
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
          - The source_version must be specified.
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
            source_version: The source file version. This is only relevant when storing
                parsed files.
            parser_name: The name of the parser, only relevant when storing parsed file.
                Assumes the default parser when not specified.

        Returns:
            A dict of the primary key and generated source version id of the file.
            Optionally returns the parser name if storing a parsed file.
            {
                "primary_key": Tuple[AllowedTypes, ...],
                "source_version": str,
                "parser_name": str, (optional)
            }

        """
        raise NotImplementedError

    def retrieve_versions(
        self,
        primary_key: Union[AllowedTypes, Tuple[AllowedTypes, ...]],
        metadata_only: bool = False,
        parsed_file: bool = False,
        parser_name: Optional[str] = None,
        latest_first: bool = True,
    ) -> Union[
        Generator[SeekableStream, None, None],
        Generator[Dict[str, AllowedTypes], None, None],
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
        """
        raise NotImplementedError

    def retrieve(
        self,
        primary_key: Union[AllowedTypes, Tuple[AllowedTypes, ...]],
        source_version: Optional[str] = None,
        metadata_only: bool = False,
        parsed_file: bool = False,
        parser_name: Optional[str] = None,
    ) -> Union[SeekableStream, Dict[str, AllowedTypes]]:
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
            SeekableStream file object or metadata item.
        """
        raise NotImplementedError

    def delete(
        self,
        primary_key: Union[AllowedTypes, Tuple[AllowedTypes, ...]],
        source_version: Optional[str] = None,
        parsed_files_only: bool = False,
        parser_name: str = "all",
    ) -> Optional[Callable[[], None]]:
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
          instead of deleting all versions right away, returns an iterator of
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
        """
        raise NotImplementedError

    def query_metadata_items(
        self,
        query_range: Optional[DatetimeRange] = None,
        index: INDEXES = INDEXES.CONTENT,
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
        """
        raise NotImplementedError

    def update_metadata_item(
        self,
        primary_key: Union[AllowedTypes, Tuple[AllowedTypes, ...]],
        source_version: str,
        update_map: Dict[str, AllowedTypes],
    ):
        """
        Updates the metadata for a given source file entry.

        Args:
            primary_key: Source file primary key.
            source_version: Source file version.
            update_map: Metadata key-val pairs to add/update.
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
