import argparse
from filelock import FileLock

from ray._private.runtime_env.packaging import Protocol

parser = argparse.ArgumentParser(
    description=("Download file from source address to the target file.")
)

parser.add_argument(
    "--proto",
    type=str,
    help="the protocol of the source address.",
)

parser.add_argument(
    "--src",
    type=str,
    help="the download source address.",
)

parser.add_argument(
    "--dest",
    type=str,
    help="the dest filename",
)


if __name__ == "__main__":
    args, remaining_args = parser.parse_known_args()

    src_address = args.src
    dest_filename = args.dest
    protocol = args.proto

    src_tp = None

    if protocol == Protocol.S3:
        try:
            import boto3
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open` and "
                "`pip install boto3` to fetch URIs in s3 "
                "bucket."
            )
        src_tp = {"client": boto3.client("s3")}
    elif protocol == Protocol.GS:
        try:
            from google.cloud import storage  # noqa: F401
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open` and "
                "`pip install google-cloud-storage` "
                "to fetch URIs in Google Cloud Storage bucket."
            )
    elif protocol == Protocol.FILE:

        def open_file(uri, mode, *, transport_params=None):
            return open(uri, mode)

    else:
        try:
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open` "
                f"to fetch {protocol.value.upper()} URIs."
            )

    with FileLock(f"{dest_filename}.lock"):
        with open_file(src_address, "rb", transport_params=src_tp) as package_zip:
            with open(dest_filename, "wb") as fin:
                fin.write(package_zip.read())
