import logging
import posixpath
import urllib.parse
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
)

import numpy as np

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pyarrow

    from ray.data.expressions import Expr


PartitionDataType = Type[Union[int, float, str, bool]]
logger = logging.getLogger(__name__)


@DeveloperAPI
class PartitionStyle(str, Enum):
    """Supported dataset partition styles.

    Inherits from `str` to simplify plain text serialization/deserialization.

    Examples:
        >>> # Serialize to JSON text.
        >>> json.dumps(PartitionStyle.HIVE)  # doctest: +SKIP
        '"hive"'

        >>> # Deserialize from JSON text.
        >>> PartitionStyle(json.loads('"hive"'))  # doctest: +SKIP
        <PartitionStyle.HIVE: 'hive'>
    """

    HIVE = "hive"
    DIRECTORY = "dir"


@DeveloperAPI
@dataclass
class Partitioning:
    """Partition scheme used to describe path-based partitions.

    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.

    For example, to read a dataset with
    `Hive-style partitions <https://athena.guide/articles/hive-style-partitioning>`_:

        >>> import ray
        >>> from ray.data.datasource.partitioning import Partitioning
        >>> ds = ray.data.read_csv(
        ...     "s3://anonymous@ray-example-data/iris.csv",
        ...     partitioning=Partitioning("hive"),
        ... )

    Instead, if your files are arranged in a directory structure such as:

    .. code::

        root/dog/dog_0.jpeg
        root/dog/dog_1.jpeg
        ...

        root/cat/cat_0.jpeg
        root/cat/cat_1.jpeg
        ...

    Then you can use directory-based partitioning:

        >>> import ray
        >>> from ray.data.datasource.partitioning import Partitioning
        >>> root = "s3://anonymous@air-example-data/cifar-10/images"
        >>> partitioning = Partitioning("dir", field_names=["class"], base_dir=root)
        >>> ds = ray.data.read_images(root, partitioning=partitioning)
    """

    #: The partition style - may be either HIVE or DIRECTORY.
    style: PartitionStyle
    #: "/"-delimited base directory that all partitioned paths should
    #: exist under (exclusive). File paths either outside of, or at the first
    #: level of, this directory will be considered unpartitioned. Specify
    #: `None` or an empty string to search for partitions in all file path
    #: directories.
    base_dir: Optional[str] = None
    #: The partition key field names (i.e. column names for tabular
    #: datasets). When non-empty, the order and length of partition key
    #: field names must match the order and length of partition values.
    #: Required when parsing DIRECTORY partitioned paths or generating
    #: HIVE partitioned paths.
    field_names: Optional[List[str]] = None
    #: A dictionary that maps partition key names to their desired data type. If not
    #: provided, the data type defaults to string.
    field_types: Optional[Dict[str, PartitionDataType]] = None
    #: Filesystem that will be used for partition path file I/O.
    filesystem: Optional["pyarrow.fs.FileSystem"] = None

    def __post_init__(self):
        if self.base_dir is None:
            self.base_dir = ""

        if self.field_types is None:
            self.field_types = {}

        self._normalized_base_dir = None
        self._resolved_filesystem = None

    @property
    def normalized_base_dir(self) -> str:
        """Returns the base directory normalized for compatibility with a filesystem."""
        if self._normalized_base_dir is None:
            self._normalize_base_dir()
        return self._normalized_base_dir

    @property
    def resolved_filesystem(self) -> "pyarrow.fs.FileSystem":
        """Returns the filesystem resolved for compatibility with a base directory."""
        if self._resolved_filesystem is None:
            self._normalize_base_dir()
        return self._resolved_filesystem

    def _normalize_base_dir(self):
        """Normalizes the partition base directory for compatibility with the
        given filesystem.

        This should be called once a filesystem has been resolved to ensure that this
        base directory is correctly discovered at the root of all partitioned file
        paths.
        """
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        paths, self._resolved_filesystem = _resolve_paths_and_filesystem(
            self.base_dir,
            self.filesystem,
        )
        assert (
            len(paths) == 1
        ), f"Expected 1 normalized base directory, but found {len(paths)}"
        normalized_base_dir = paths[0]
        if len(normalized_base_dir) and not normalized_base_dir.endswith("/"):
            normalized_base_dir += "/"
        self._normalized_base_dir = normalized_base_dir


@DeveloperAPI
class PathPartitionParser:
    """Partition parser for path-based partition formats.

    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.

    Two path partition formats are currently supported - `HIVE` and `DIRECTORY`.

    For `HIVE` Partitioning, all partition directories under the base directory
    will be discovered based on `{key1}={value1}/{key2}={value2}` naming
    conventions. Key/value pairs do not need to be presented in the same
    order across all paths. Directory names nested under the base directory that
    don't follow this naming condition will be considered unpartitioned. If a
    partition filter is defined, then it will be called with an empty input
    dictionary for each unpartitioned file.

    For `DIRECTORY` Partitioning, all directories under the base directory will
    be interpreted as partition values of the form `{value1}/{value2}`. An
    accompanying ordered list of partition field names must also be provided,
    where the order and length of all partition values must match the order and
    length of field names. Files stored directly in the base directory will
    be considered unpartitioned. If a partition filter is defined, then it will
    be called with an empty input dictionary for each unpartitioned file. For
    example, if the base directory is `"foo"`, then `"foo.csv"` and `"foo/bar.csv"`
    would be considered unpartitioned files but `"foo/bar/baz.csv"` would be associated
    with partition `"bar"`. If the base directory is undefined, then `"foo.csv"` would
    be unpartitioned, `"foo/bar.csv"` would be associated with partition `"foo"`, and
    "foo/bar/baz.csv" would be associated with partition `("foo", "bar")`.
    """

    @staticmethod
    def of(
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        field_types: Optional[Dict[str, PartitionDataType]] = None,
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
            field_types: A dictionary that maps partition key names to their desired
                data type. If not provided, the data type default to string.
            filesystem: Filesystem that will be used for partition path file I/O.

        Returns:
            The new path-based partition parser.
        """
        scheme = Partitioning(style, base_dir, field_names, field_types, filesystem)
        return PathPartitionParser(scheme)

    def __init__(self, partitioning: Partitioning):
        """Creates a path-based partition parser.

        Args:
            partitioning: The path-based partition scheme. The parser starts
                searching for partitions from this scheme's base directory. File paths
                outside the base directory will be considered unpartitioned. If the
                base directory is `None` or an empty string then this will search for
                partitions in all file path directories. Field names are required for
                DIRECTORY partitioning, and optional for HIVE partitioning. When
                non-empty, the order and length of partition key field names must match
                the order and length of partition directories discovered.
        """
        style = partitioning.style
        field_names = partitioning.field_names
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
        self._scheme = partitioning

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
        partitions: Dict[str, str] = self._parser_fn(dir_path)

        for field, data_type in self._scheme.field_types.items():
            partitions[field] = _cast_value(partitions[field], data_type)

        return partitions

    def evaluate_predicate_on_partition(self, path: str, predicate: "Expr") -> bool:
        """Evaluate a predicate expression against partition values from a path.

        This method enables partition pruning by evaluating predicates that reference
        partition columns against the partition values parsed from file paths.

        Args:
            path: File path to parse partition values from.
            predicate: Expression that references partition columns.

        Returns:
            True if the partition satisfies the predicate (should read the file),
            False if it doesn't (can skip the file for partition pruning).
        """
        import pyarrow as pa

        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            NativeExpressionEvaluator,
        )

        # Parse partition values from the file path
        partition_values = self(path)

        if not partition_values:
            # Unpartitioned file - exclude it when filtering on partition columns
            # If the predicate references partition columns and the file doesn't have
            # partition values in its path, we can't determine if it matches
            return False

        try:
            # Create a single-row table with partition values
            partition_table = pa.table(
                {col: [val] for col, val in partition_values.items()}
            )

            # Evaluate using Ray Data's native evaluator
            evaluator = NativeExpressionEvaluator(partition_table)
            result = evaluator.visit(predicate)

            # Extract boolean result from array-like types
            # Check for specific array types to avoid issues with strings (which are iterable)
            if isinstance(result, (pa.Array, pa.ChunkedArray, np.ndarray)):
                return bool(result[0])

            # Import pandas here to avoid circular dependencies
            import pandas as pd

            if isinstance(result, pd.Series):
                return bool(result.iloc[0])

            # Scalar result (shouldn't happen with table evaluation, but handle conservatively)
            return bool(result)
        except Exception:
            logger.debug(
                "Failed to evaluate predicate on partition for path %s, "
                "conservatively including file.",
                path,
                exc_info=True,
            )
            return True

    @property
    def scheme(self) -> Partitioning:
        """Returns the partitioning for this parser."""
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
        # NOTE: PyArrow URL-encodes partition values when writing to cloud storage. To
        #       ensure the values are consistent when you read them back, we need to
        #       URL-decode them. See https://github.com/apache/arrow/issues/34905.
        kv_pairs = [[key, urllib.parse.unquote(value)] for key, value in kv_pairs]

        field_names = self._scheme.field_names
        if field_names and kv_pairs:
            if len(kv_pairs) != len(field_names):
                raise ValueError(
                    f"Expected {len(field_names)} partition value(s) but found "
                    f"{len(kv_pairs)}: {kv_pairs}."
                )
            for i, field_name in enumerate(field_names):
                if kv_pairs[i][0] != field_name:
                    raise ValueError(
                        f"Expected partition key {field_name} but found "
                        f"{kv_pairs[i][0]}"
                    )
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

        if dirs and len(dirs) != len(field_names):
            raise ValueError(
                f"Expected {len(field_names)} partition value(s) but found "
                f"{len(dirs)}: {dirs}."
            )

        if not dirs:
            return {}
        return {
            field: directory
            for field, directory in zip(field_names, dirs)
            if field is not None
        }


@PublicAPI(stability="beta")
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
        field_types: Optional[Dict[str, PartitionDataType]] = None,
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

                .. code:: python

                    lambda d: True if d else False

                This raises an assertion error for any unpartitioned file found:

                .. code:: python

                    def do_assert(val, msg):
                        assert val, msg

                    lambda d: do_assert(d, "Expected all files to be partitioned!")

                And this only reads files from January, 2022 partitions:

                .. code:: python

                    lambda d: d["month"] == "January" and d["year"] == "2022"

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
            field_types: A dictionary that maps partition key names to their desired
                data type. If not provided, the data type defaults to string.
            filesystem: Filesystem that will be used for partition path file I/O.

        Returns:
            The new path-based partition filter.
        """
        scheme = Partitioning(style, base_dir, field_names, field_types, filesystem)
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
                ``lambda d: assert d, "Expected all files to be partitioned!"``
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
            filtered_paths = [path for path in paths if self.apply(path)]
        return filtered_paths

    def apply(self, path: str) -> bool:
        return self._filter_fn(self._parser(path))

    @property
    def parser(self) -> PathPartitionParser:
        """Returns the path partition parser for this filter."""
        return self._parser


def _cast_value(value: str, data_type: PartitionDataType) -> Any:
    if data_type is int:
        return int(value)
    elif data_type is float:
        return float(value)
    elif data_type is bool:
        return value.lower() == "true"
    else:
        return value
