import ray
from urllib.parse import urlparse
from utils import get_spark_session, is_in_databricks_runtime


def load_spark_dataset(path, saved_format="delta"):
    """
    Returns a Ray dataset that loads data from a saved spark dataset path.

    Args:
        path: The path to the saved spark dataset.
        saved_format: The format of the saved dataset, only 'parquet' and
            'delta' format are supported.
    """
    spark = get_spark_session()
    assert saved_format in ["parquet", "delta"], \
        f"Unsupported saved format {saved_format}"

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
        file_paths = [
            "/dbfs" + urlparse(p).path
            for p in file_paths
        ]
    return ray.data.read_parquet(file_paths)
