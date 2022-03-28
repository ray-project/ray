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
class PathPartitionParser:
    """Partition parser and utilities for path-based partition formats.

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

    def __init__(
        self,
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filter_fn: Optional[Callable[[Dict[str, str]], bool]] = None,
    ):
        """Creates a new path-based dataset partition scheme.

        Args:
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory to start searching for partitions
                (exclusive). File paths outside of this directory will be considered
                unpartitioned. Specify `None` or an empty string to search for
                partitions in all file path directories.
            field_names: The partition key names. Required for DIRECTORY partitioning.
                Optional for HIVE partitioning. When non-empty, the order and length of
                of partition key field names must match the order and length of
                partition directories discovered. Partition key field names are not
                required to exist in the dataset schema.
            filter_fn: Callback used to filter partitions. Takes a dictionary mapping
                partition keys to values as input. Unpartitioned files are denoted with
                an empty input dictionary. Returns `True` to read a partition or `False`
                to skip it. Partition keys and values are always strings read from the
                filesystem path. For example, this removes all unpartitioned files:
                ``lambda d: True if d else False``
                This raises an assertion error for any unpartitioned file found:
                ``def do_assert(val, msg):
                    assert val, msg
                  lambda d: do_assert(d, "Expected all files to be partitioned!")``
                And this only reads files from January, 2022 partitions:
                ``lambda d: d["month"] == "January" and d["year"] == "2022"``
        """
        self._style = style
        self._base_dir = base_dir or ""
        if style == PartitionStyle.DIRECTORY and not field_names:
            raise ValueError(
                "Directory partitioning requires a corresponding list of "
                "partition key field names. Please retry your request with one "
                "or more field names specified."
            )
        self._field_names = field_names
        self._filter_fn = filter_fn
        kv_parser_map = {
            PartitionStyle.HIVE: self._parse_hive_path,
            PartitionStyle.DIRECTORY: self._parse_dir_path,
        }
        self._parser_fn: Callable[[str], Dict[str, str]] = kv_parser_map.get(style)
        if self._parser_fn is None:
            raise ValueError(
                f"Unsupported partition style: {style}. "
                f"Supported styles: {kv_parser_map.keys()}"
            )

    @property
    def style(self) -> PartitionStyle:
        return self._style

    @property
    def base_dir(self) -> Optional[str]:
        return self._base_dir

    @property
    def field_names(self) -> Optional[List[str]]:
        return self._field_names

    def filter_paths(
        self,
        paths: List[str],
        filesystem: "pyarrow.fs.FileSystem",
    ) -> List[str]:
        """Removes all paths that don't pass this partition scheme's partition
        filter. If no partition filter is set, then returns all input paths. If
        a base directory is set, then only paths under this base directory will
        be parsed for partitions. All paths outside of this base directory will
        automatically be considered unpartitioned, and passed into the filter
        function as empty dictionaries.

        Also normalizes the partition base directory for compatibility with the
        given filesystem before applying the filter.

        Args:
            filesystem: Filesystem that will be used to read all file paths.
            paths: Paths to pass through the partition filter function. All
                paths should be normalized for compatibility with the given
                filesystem.
        Returns:
            List of paths that pass the partition filter, or all paths if no
            partition filter is defined.
        """
        filtered_paths = paths
        if self._filter_fn is not None:
            self._normalize_base_dir(filesystem)
            filtered_paths = [
                path for path in paths if self._filter_fn(self._parser_fn(path))
            ]
        return filtered_paths

    def _normalize_base_dir(self, filesystem: "pyarrow.fs.FileSystem"):
        """Normalizes the partition base directory for compatibility with the
        given filesystem.

        This should be called once a filesystem has been resolved for reading all
        dataset file paths to ensure that this base directory is correctly discovered
        at the root of all partitioned file paths.
        """
        from ray.data.datasource.file_based_datasource import (
            _resolve_paths_and_filesystem,
        )

        paths, _ = _resolve_paths_and_filesystem(
            self._base_dir,
            filesystem,
        )
        assert (
            len(paths) == 1
        ), f"Expected 1 normalized base directory, but found {len(paths)}"
        new_base_dir = paths[0]
        if len(new_base_dir) and not new_base_dir.endswith("/"):
            new_base_dir += "/"
        self._base_dir = new_base_dir

    def _parse_hive_path(self, path: str) -> Dict[str, str]:
        """Hive partition path parser.

        Returns a dictionary mapping partition keys to values given a hive-style
        partition path of the form "{key1}={value1}/{key2}={value2}/..." or an empty
        dictionary for unpartitioned files.
        """
        if not path.startswith(self._base_dir):
            return {}
        path = path[len(self._base_dir) :]
        dir_path = posixpath.dirname(path)
        dirs = [d for d in dir_path.split("/") if d and (d.count("=") == 1)]
        kv_pairs = [d.split("=") for d in dirs] if dirs else []
        if self._field_names:
            assert len(kv_pairs) == len(self._field_names)
            for i, field_name in enumerate(self._field_names):
                assert (
                    kv_pairs[i][0] == field_name
                ), f"Expected partition key {field_name} but found {kv_pairs[i][0]}"
        return dict(kv_pairs)

    def _parse_dir_path(self, path: str) -> Dict[str, str]:
        """Directory partition path parser.

        Returns a dictionary mapping directory partition keys to values from a
        partition path of the form "{value1}/{value2}/..." or an empty dictionary for
        unpartitioned files.

        Requires a corresponding ordered list of partition key field names to map the
        correct key to each value.
        """
        if not path.startswith(self._base_dir):
            return {}
        path = path[len(self._base_dir) :]
        dir_path = posixpath.dirname(path)
        dirs = [d for d in dir_path.split("/") if d]
        assert not dirs or len(dirs) == len(self._field_names), (
            f"Expected {len(self._field_names)} partition value(s) but found "
            f"{len(dirs)}: {dirs}."
        )
        return {self._field_names[i]: d for i, d in enumerate(dirs)} if dirs else {}
