

def is_remote_path(path: str) -> bool:
    """
    Determines if the given path is a remote path (S3 or GCS) or a local path.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path is remote (S3 or GCS), False if it's local.
    """
    if path.startswith("s3://") or path.startswith("gs://"):
        return True
    return False
