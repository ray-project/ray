import posixpath
from enum import Enum
from typing import (
    Optional,
    List,
    Callable,
    Dict,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import pyarrow

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class PartitionStyle(str, Enum):
    """Supported dataset partition styles.

    Inherits from `str` to simplify plain text serialization/deserialization.

    Examples:
        >>> # Serialize to JSON text.
        >>> json.dumps(PartitionStyle.HIVE)  # "hive"

        >>> # Deserialize from JSON text.
        >>> PartitionStyle(json.loads('"hive"'))  # PartitionStyle.HIVE
    """

    HIVE = "hive"
    DIRECTORY = "dir"


@DeveloperAPI
class PathPartitionScheme:
    """Partition scheme used to describe path-based partitions.

    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.
    """

    def __init__(
        self,
        style: PartitionStyle,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        """Creates a new path-based dataset partition scheme.

        Args:
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory that all partitioned paths should
                exist under (exclusive). File paths either outside of, or at the first
                level of, this directory will be considered unpartitioned. Specify
                `None` or an empty string to search for partitions in all file path
                directories.
            field_names: The partition key field names (i.e. column names for tabular
                datasets). When non-empty, the order and length of partition key
                field names must match the order and length of partition values.
                Required when parsing DIRECTORY partitioned paths or generating
                HIVE partitioned paths.
            filesystem: Filesystem that will be used for partition path file I/O.
        """
        self._style = style
        self._base_dir = base_dir or ""
        self._field_names = field_names
        self._filesystem = filesystem
        self._normalize_base_dir()

    @property
    def style(self) -> PartitionStyle:
        """Gets the path partitioning style."""
        return self._style

    @property
    def base_dir(self) -> str:
        """Gets the original base directory supplied during object construction."""
        return self._base_dir

    @property
    def normalized_base_dir(self) -> str:
        """Returns the base directory normalized for compatibility with a filesystem."""
        return self._normalized_base_dir

    @property
    def field_names(self) -> Optional[List[str]]:
        """Gets the partition key field names."""
        return self._field_names

    @property
    def filesystem(self) -> Optional["pyarrow.fs.FileSystem"]:
        """Gets the original filesystem supplied during object construction."""
        return self._filesystem

    @property
    def resolved_filesystem(self) -> "pyarrow.fs.FileSystem":
        """Returns the filesystem resolved for compatibility with a base directory."""
        return self._resolved_filesystem

    def _normalize_base_dir(self):
        """Normalizes the partition base directory for compatibility with the
        given filesystem.

        This should be called once a filesystem has been resolved to ensure that this
        base directory is correctly discovered at the root of all partitioned file
        paths.
        """
        from ray.data.datasource.file_based_datasource import (
            _resolve_paths_and_filesystem,
        )

        paths, self._resolved_filesystem = _resolve_paths_and_filesystem(
            self._base_dir,
            self._filesystem,
        )
        assert (
            len(paths) == 1
        ), f"Expected 1 normalized base directory, but found {len(paths)}"
        normalized_base_dir = paths[0]
        if len(normalized_base_dir) and not normalized_base_dir.endswith("/"):
            normalized_base_dir += "/"
        self._normalized_base_dir = normalized_base_dir


@DeveloperAPI
class PathPartitionEncoder:
    """Callable that generates directory path strings for path-based partition formats.

    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.

    Two path partition formats are currently supported - HIVE and DIRECTORY.

    For HIVE Partitioning, all partition directories will be generated using a
    "{key1}={value1}/{key2}={value2}" naming convention under the base directory.
    An accompanying ordered list of partition key field names must also be
    provided, where the order and length of all partition values must match the
    order and length of field names

    For DIRECTORY Partitioning, all directories will be generated from partition
    values using a "{value1}/{value2}" naming convention under the base directory.
    """

    @staticmethod
    def of(
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> "PathPartitionEncoder":
        """Creates a new partition path encoder.
        Args:
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory that all partition paths will be
                generated under (exclusive).
            field_names: The partition key field names (i.e. column names for tabular
                datasets). Required for HIVE partition paths, optional for DIRECTORY
                partition paths. When non-empty, the order and length of partition key
                field names must match the order and length of partition values.
            filesystem: Filesystem that will be used for partition path file I/O.

        Returns:
            The new partition path encoder.
        """
        scheme = PathPartitionScheme(style, base_dir, field_names, filesystem)
        return PathPartitionEncoder(scheme)

    def __init__(self, path_partition_scheme: PathPartitionScheme):
        """Creates a new partition path encoder.

        Args:
            path_partition_scheme: The path-based partition scheme. All partition paths
                will be generated under this scheme's base directory. Field names are
                required for HIVE partition paths, optional for DIRECTORY partition
                paths. When non-empty, the order and length of partition key field
                names must match the order and length of partition values.
        """
        style = path_partition_scheme.style
        field_names = path_partition_scheme.field_names
        if style == PartitionStyle.HIVE and not field_names:
            raise ValueError(
                "Hive partition path generation requires a corresponding list of "
                "partition key field names. Please retry your request with one "
                "or more field names specified."
            )
        generators = {
            PartitionStyle.HIVE: self._as_hive_partition_dirs,
            PartitionStyle.DIRECTORY: self._as_directory_partition_dirs,
        }
        self._encoder_fn: Callable[[List[str]], List[str]] = generators.get(style)
        if self._encoder_fn is None:
            raise ValueError(
                f"Unsupported partition style: {style}. "
                f"Supported styles: {generators.keys()}"
            )
        self._scheme = path_partition_scheme

    def __call__(self, partition_values: List[str]) -> str:
        """Returns the partition directory path for the given partition value strings.

        All files for this partition should be written to this directory. If a base
        directory is set, then the partition directory path returned will be rooted in
        this base directory.

        Args:
            partition_values: The partition value strings to include in the partition
                path. For HIVE partition paths, the order and length of partition
                values must match the order and length of partition key field names.

        Returns:
            Partition directory path for the given partition values.
        """
        partition_dirs = self._as_partition_dirs(partition_values)
        return posixpath.join(self._scheme.normalized_base_dir, *partition_dirs)

    @property
    def scheme(self) -> PathPartitionScheme:
        """Returns the path partition scheme for this encoder."""
        return self._scheme

    def _as_hive_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates HIVE directory names for the given values."""
        field_names = self._scheme.field_names
        return [f"{field_names[i]}={val}" for i, val in enumerate(values)]

    def _as_directory_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates DIRECTORY partition directory names for the given values."""
        return values

    def _as_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates a list of partition directory names for the given values."""
        field_names = self._scheme.field_names
        if field_names:
            assert len(values) == len(field_names), (
                f"Expected {len(field_names)} partition value(s) but found "
                f"{len(values)}: {values}."
            )
        return self._encoder_fn(values)


@DeveloperAPI
class PathPartitionParser:
    """Partition parser for path-based partition formats.

    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.

    Two path partition formats are currently supported - HIVE and DIRECTORY.

    For HIVE Partitioning, all partition directories under the base directory
    will be discovered based on "{key1}={value1}/{key2}={value2}" naming
    conventions. Key/value pairs do not need to be presented in the same
    order across all paths. Directory names nested under the base directory that
    don't follow this naming condition will be considered unpartitioned. If a
    partition filter is defined, then it will be called with an empty input
    dictionary for each unpartitioned file.

    For DIRECTORY Partitioning, all directories under the base directory will
    be interpreted as partition values of the form "{value1}/{value2}". An
    accompanying ordered list of partition field names must also be provided,
    where the order and length of all partition values must match the order and
    length of field names. Files stored directly in the base directory will
    be considered unpartitioned. If a partition filter is defined, then it will
    be called with an empty input dictionary for each unpartitioned file. For
    example, if the base directory is "foo" then "foo.csv" and "foo/bar.csv" would be
    considered unpartitioned files but "foo/bar/baz.csv" would be associated with
    partition "bar". If the base directory is undefined, then "foo.csv" would be
    unpartitioned, "foo/bar.csv" would be associated with partition "foo", and
    "foo/bar/baz.csv" would be associated with partition ("foo", "bar").
    """

    @staticmethod
    def of(
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> "PathPartitionParser":
        """Creates a path-based partition parser using a flattened argument list.

        Args:
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory to start searching for partitions
                (exclusive). File paths outside of this directory will be considered
                unpartitioned. Specify `None` or an empty string to search for
                partitions in all file path directories.
            field_names: The partition key names. Required for DIRECTORY partitioning.
                Optional for HIVE partitioning. When non-empty, the order and length of
                partition key field names must match the order and length of partition
                directories discovered. Partition key field names are not required to
                exist in the dataset schema.
            filesystem: Filesystem that will be used for partition path file I/O.

        Returns:
            The new path-based partition parser.
        """
        scheme = PathPartitionScheme(style, base_dir, field_names, filesystem)
        return PathPartitionParser(scheme)

    def __init__(self, path_partition_scheme: PathPartitionScheme):
        """Creates a path-based partition parser.

        Args:
            path_partition_scheme: The path-based partition scheme. The parser starts
                searching for partitions from this scheme's base directory. File paths
                outside the base directory will be considered unpartitioned. If the
                base directory is `None` or an empty string then this will search for
                partitions in all file path directories. Field names are required for
                DIRECTORY partitioning, and optional for HIVE partitioning. When
                non-empty, the order and length of partition key field names must match
                the order and length of partition directories discovered.
        """
        style = path_partition_scheme.style
        field_names = path_partition_scheme.field_names
        if style == PartitionStyle.DIRECTORY and not field_names:
            raise ValueError(
                "Directory partitioning requires a corresponding list of "
                "partition key field names. Please retry your request with one "
                "or more field names specified."
            )
        parsers = {
            PartitionStyle.HIVE: self._parse_hive_path,
            PartitionStyle.DIRECTORY: self._parse_dir_path,
        }
        self._parser_fn: Callable[[str], Dict[str, str]] = parsers.get(style)
        if self._parser_fn is None:
            raise ValueError(
                f"Unsupported partition style: {style}. "
                f"Supported styles: {parsers.keys()}"
            )
        self._scheme = path_partition_scheme

    def __call__(self, path: str) -> Dict[str, str]:
        """Parses partition keys and values from a single file path.

        Args:
            path: Input file path to parse.
        Returns:
            Dictionary mapping directory partition keys to values from the input file
            path. Returns an empty dictionary for unpartitioned files.
        """
        dir_path = self._dir_path_trim_base(path)
        if dir_path is None:
            return {}
        return self._parser_fn(dir_path)

    @property
    def scheme(self) -> PathPartitionScheme:
        """Returns the path partition scheme for this parser."""
        return self._scheme

    def _dir_path_trim_base(self, path: str) -> Optional[str]:
        """Trims the normalized base directory and returns the directory path.

        Returns None if the path does not start with the normalized base directory.
        Simply returns the directory path if the base directory is undefined.
        """
        if not path.startswith(self._scheme.normalized_base_dir):
            return None
        path = path[len(self._scheme.normalized_base_dir) :]
        return posixpath.dirname(path)

    def _parse_hive_path(self, dir_path: str) -> Dict[str, str]:
        """Hive partition path parser.

        Returns a dictionary mapping partition keys to values given a hive-style
        partition path of the form "{key1}={value1}/{key2}={value2}/..." or an empty
        dictionary for unpartitioned files.
        """
        dirs = [d for d in dir_path.split("/") if d and (d.count("=") == 1)]
        kv_pairs = [d.split("=") for d in dirs] if dirs else []
        field_names = self._scheme.field_names
        if field_names and kv_pairs:
            assert len(kv_pairs) == len(field_names), (
                f"Expected {len(field_names)} partition value(s) but found "
                f"{len(kv_pairs)}: {kv_pairs}."
            )
            for i, field_name in enumerate(field_names):
                assert (
                    kv_pairs[i][0] == field_name
                ), f"Expected partition key {field_name} but found {kv_pairs[i][0]}"
        return dict(kv_pairs)

    def _parse_dir_path(self, dir_path: str) -> Dict[str, str]:
        """Directory partition path parser.

        Returns a dictionary mapping directory partition keys to values from a
        partition path of the form "{value1}/{value2}/..." or an empty dictionary for
        unpartitioned files.

        Requires a corresponding ordered list of partition key field names to map the
        correct key to each value.
        """
        dirs = [d for d in dir_path.split("/") if d]
        field_names = self._scheme.field_names
        assert not dirs or len(dirs) == len(field_names), (
            f"Expected {len(field_names)} partition value(s) but found "
            f"{len(dirs)}: {dirs}."
        )
        return {field_names[i]: d for i, d in enumerate(dirs)} if dirs else {}


class PathPartitionFilter:
    """Partition filter for path-based partition formats.

    Used to explicitly keep or reject files based on a custom filter function that
    takes partition keys and values parsed from the file's path as input.
    """

    @staticmethod
    def of(
        filter_fn: Callable[[Dict[str, str]], bool],
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> "PathPartitionFilter":
        """Creates a path-based partition filter using a flattened argument list.

        Args:
            filter_fn: Callback used to filter partitions. Takes a dictionary mapping
                partition keys to values as input. Unpartitioned files are denoted with
                an empty input dictionary. Returns `True` to read a file for that
                partition or `False` to skip it. Partition keys and values are always
                strings read from the filesystem path. For example, this removes all
                unpartitioned files:
                ``lambda d: True if d else False``
                This raises an assertion error for any unpartitioned file found:
                ``def do_assert(val, msg):
                    assert val, msg
                  lambda d: do_assert(d, "Expected all files to be partitioned!")``
                And this only reads files from January, 2022 partitions:
                ``lambda d: d["month"] == "January" and d["year"] == "2022"``
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory to start searching for partitions
                (exclusive). File paths outside of this directory will be considered
                unpartitioned. Specify `None` or an empty string to search for
                partitions in all file path directories.
            field_names: The partition key names. Required for DIRECTORY partitioning.
                Optional for HIVE partitioning. When non-empty, the order and length of
                partition key field names must match the order and length of partition
                directories discovered. Partition key field names are not required to
                exist in the dataset schema.
            filesystem: Filesystem that will be used for partition path file I/O.

        Returns:
            The new path-based partition filter.
        """
        scheme = PathPartitionScheme(style, base_dir, field_names, filesystem)
        path_partition_parser = PathPartitionParser(scheme)
        return PathPartitionFilter(path_partition_parser, filter_fn)

    def __init__(
        self,
        path_partition_parser: PathPartitionParser,
        filter_fn: Callable[[Dict[str, str]], bool],
    ):
        """Creates a new path-based partition filter based on a parser.

        Args:
            path_partition_parser: The path-based partition parser.
            filter_fn: Callback used to filter partitions. Takes a dictionary mapping
                partition keys to values as input. Unpartitioned files are denoted with
                an empty input dictionary. Returns `True` to read a file for that
                partition or `False` to skip it. Partition keys and values are always
                strings read from the filesystem path. For example, this removes all
                unpartitioned files:
                ``lambda d: True if d else False``
                This raises an assertion error for any unpartitioned file found:
                ``def do_assert(val, msg):
                    assert val, msg
                  lambda d: do_assert(d, "Expected all files to be partitioned!")``
                And this only reads files from January, 2022 partitions:
                ``lambda d: d["month"] == "January" and d["year"] == "2022"``
        """
        self._parser = path_partition_parser
        self._filter_fn = filter_fn

    def __call__(self, paths: List[str]) -> List[str]:
        """Returns all paths that pass this partition scheme's partition filter.

        If no partition filter is set, then returns all input paths. If a base
        directory is set, then only paths under this base directory will be parsed
        for partitions. All paths outside of this base directory will automatically
        be considered unpartitioned, and passed into the filter function as empty
        dictionaries.

        Also normalizes the partition base directory for compatibility with the
        given filesystem before applying the filter.

        Args:
            paths: Paths to pass through the partition filter function. All
                paths should be normalized for compatibility with the given
                filesystem.
        Returns:
            List of paths that pass the partition filter, or all paths if no
            partition filter is defined.
        """
        filtered_paths = paths
        if self._filter_fn is not None:
            filtered_paths = [
                path for path in paths if self._filter_fn(self._parser(path))
            ]
        return filtered_paths

    @property
    def parser(self) -> PathPartitionParser:
        """Returns the path partition parser for this filter."""
        return self._parser
