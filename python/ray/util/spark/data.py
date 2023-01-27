import ray
from urllib.parse import urlparse
from ray.util.spark.utils import get_spark_session, is_in_databricks_runtime


def _convert_dbfs_path_to_local_path(dbfs_path):
    parsed_path = urlparse(dbfs_path)
    assert parsed_path.scheme.lower() == "dbfs", "dbfs path is required."
    return "/dbfs" + parsed_path.path


def load_spark_dataset(path, saved_format):
    """
    Returns a Ray dataset that loads data from a saved spark dataset path.

    Args:
        path: The path to the saved spark dataset.
        saved_format: The format of the saved dataset, only 'parquet' and
            'delta' format are supported. 'delta' format is only supported
            on Databricks runtime.
    """
    spark = get_spark_session()
    assert saved_format in [
        "parquet",
        "delta",
    ], f"Unsupported saved format {saved_format}"

    # NB: The delta format dataset directory might contain some parquet files
    # that holds old version data, so we must call `inputFiles()` API to get
    # parquet files that contains the current version data.
    file_paths = spark.read.load(path, format=saved_format).inputFiles()

    if len(file_paths) == 0:
        raise ValueError("The directory does not contain valid parquet files.")

    scheme = urlparse(file_paths[0]).scheme

    if is_in_databricks_runtime() and scheme.lower() == "dbfs":
        # On databricks runtime, spark dataset by default is saved to dbfs
        # filesystem. PyArrow does not support to read data from dbfs
        # filesystem, but databricks runtime mounts dbfs path "dbfs:/xx/yy"
        # to local filesystem path "/dbfs/xx/yy",
        # so that here we convert the dbfs path to the corresponding
        # local "/dbfs/..." path.
        file_paths = [_convert_dbfs_path_to_local_path(p) for p in file_paths]
    return ray.data.read_parquet(file_paths)
