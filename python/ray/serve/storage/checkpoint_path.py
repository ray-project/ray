from urllib.parse import parse_qsl, urlparse

from ray.serve.utils import logger
from ray.serve.constants import DEFAULT_CHECKPOINT_PATH
from ray.serve.storage.kv_store import RayInternalKVStore, RayLocalKVStore, RayS3KVStore
from ray.serve.storage.kv_store_base import KVStoreBase
from ray.serve.storage.ray_gcs_kv_store import RayGcsKVStore
from ray._private.utils import import_attr


def make_kv_store(checkpoint_path, namespace):
    """Create KVStore instance based on checkpoint_path configuration"""

    if checkpoint_path == DEFAULT_CHECKPOINT_PATH:
        logger.info(
            "Using RayInternalKVStore for controller " "checkpoint and recovery."
        )
        return RayInternalKVStore(namespace)
    else:
        parsed_url = urlparse(checkpoint_path)
        if parsed_url.scheme not in {"gs", "s3", "file", "custom"}:
            raise ValueError(
                f"Checkpoint must be one of `{DEFAULT_CHECKPOINT_PATH}`, "
                "`file://path...`, `gs://path...`, `s3://path...`, or "
                "`custom://my_module.ClassName?arg1=val1`. But it is "
                f"{checkpoint_path}"
            )

        if parsed_url.scheme == "file":
            db_path = parsed_url.netloc + parsed_url.path
            logger.info(
                "Using RayLocalKVStore for controller "
                f"checkpoint and recovery: path={db_path}"
            )
            return RayLocalKVStore(namespace, db_path)

        if parsed_url.scheme == "gs":
            bucket = parsed_url.netloc
            # We need to strip leading "/" in path as right key to use in
            # gcs. Ex: gs://bucket/folder/file.zip -> key = "folder/file.zip"
            prefix = parsed_url.path.lstrip("/")
            logger.info(
                "Using Ray GCS KVStore for controller checkpoint and"
                " recovery: "
                f"bucket={bucket} checkpoint_path={checkpoint_path}"
            )
            return RayGcsKVStore(
                namespace,
                bucket=bucket,
                prefix=prefix,
            )

        if parsed_url.scheme == "s3":
            bucket = parsed_url.netloc
            # We need to strip leading "/" in path as right key to use in
            # boto3. Ex: s3://bucket/folder/file.zip -> key = "folder/file.zip"
            prefix = parsed_url.path.lstrip("/")
            logger.info(
                "Using Ray S3 KVStore for controller checkpoint and recovery: "
                f"bucket={bucket} checkpoint_path={checkpoint_path}"
            )
            return RayS3KVStore(
                namespace,
                bucket=bucket,
                prefix=prefix,
            )

        if parsed_url.scheme == "custom":
            kwargs = dict(parse_qsl(parsed_url.query))

            # Prepare the parameters to initialize imported class.
            checkpoint_provider = parsed_url.netloc
            KVStoreClass = import_attr(checkpoint_provider)
            if not issubclass(KVStoreClass, KVStoreBase):
                raise ValueError(
                    f"{KVStoreClass} doesn't inherit from "
                    "`ray.serve.storage.kv_store_base.KVStoreBase`."
                )

            logger.info(
                f"Using {checkpoint_provider} for controller checkpoint and "
                f"recovery: kwargs={kwargs}"
            )
            return KVStoreClass(namespace=namespace, **kwargs)

    raise RuntimeError("This shouldn't be reachable.")
