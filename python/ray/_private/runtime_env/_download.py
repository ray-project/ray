import argparse
import smart_open

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

def open_file(uri, mode, *, transport_params=None):
    return open(uri, mode)

src_handlers = {
    "https": smart_open.open,
    "s3": smart_open.open,
    "gs": smart_open.open,
    "file": open_file,
}

src_tp = {
    "https": None,
    "s3": {"client": boto3.client("s3")},
    "gs": None,
    "file": None,
}

if __name__ == "__main__":
    args, remaining_args = parser.parse_known_args()

    src_address = args.src
    dest_filename = args.dest
    protocol = args.proto

    with FileLock(f"{dest_filename}.lock"):
        with file_handlers[protocol](src_address, "rb", transport_params=src_tp[protocol]) as package_zip:
            with open(dest_filename, "wb") as fin:
                fin.write(package_zip.read())