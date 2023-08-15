from typing import List, Optional
import os
import subprocess
import logging

logger = logging.getLogger(__name__)


def get_hash_from_bucket(
    bucket_uri: str, s3_sync_args: Optional[List[str]] = None
) -> str:

    s3_sync_args = s3_sync_args or []
    subprocess.run(
        ["aws", "s3", "cp", "--quiet"]
        + s3_sync_args
        + [os.path.join(bucket_uri, "refs", "main"), "."]
    )

    with open(os.path.join(".", "main"), "r") as f:
        f_hash = f.read().strip()

    return f_hash


def get_checkpoint_and_refs_dir(
    model_id: str,
    bucket_uri: str,
    s3_sync_args: Optional[List[str]] = None,
    mkdir: bool = False,
) -> str:

    from transformers.utils.hub import TRANSFORMERS_CACHE

    f_hash = get_hash_from_bucket(bucket_uri, s3_sync_args)

    path = os.path.join(TRANSFORMERS_CACHE, f"models--{model_id.replace('/', '--')}")

    refs_dir = os.path.join(path, "refs")
    checkpoint_dir = os.path.join(path, "snapshots", f_hash)

    if mkdir:
        os.makedirs(refs_dir, exist_ok=True)
        os.makedirs(checkpoint_dir, exist_ok=True)

    return checkpoint_dir, refs_dir


def get_download_path(model_id: str):
    from transformers.utils.hub import TRANSFORMERS_CACHE

    path = os.path.join(TRANSFORMERS_CACHE, f"models--{model_id.replace('/', '--')}")
    return path


def download_model(
    model_id: str,
    bucket_uri: str,
    s3_sync_args: Optional[List[str]] = None,
    tokenizer_only: bool = False,
) -> None:
    """
    Download a model from an S3 bucket and save it in TRANSFORMERS_CACHE for
    seamless interoperability with Hugging Face's Transformers library.

    The downloaded model may have a 'hash' file containing the commit hash corresponding
    to the commit on Hugging Face Hub.
    """
    s3_sync_args = s3_sync_args or []
    path = get_download_path(model_id)

    cmd = (
        ["aws", "s3", "sync"]
        + s3_sync_args
        + (["--exclude", "*", "--include", "*token*"] if tokenizer_only else [])
        + [bucket_uri, path]
    )
    print(f"RUN({cmd})")
    subprocess.run(cmd)
    print("done")


def get_mirror_link(model_id: str) -> str:
    return f"s3://llama-2-weights/models--{model_id.replace('/', '--')}"
