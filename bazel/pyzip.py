#!/usr/bin/env python3

# This script is used to zip a directory into a zip file.
# It only uses python standard library, so it can be portable and used in bazel.

import os
import os.path
import sys
import zipfile

# Everything in the zip file is stored with this timestamp.
# This makes the zip file building deterministic and reproducible.
_TIMESTAMP = (2020, 1, 1, 0, 0, 0)

_UNIX_DIR_BIT = 0o040000
_MSDOS_DIR_BIT = 0x10
_DIR_BIT = (_UNIX_DIR_BIT << 16) | _MSDOS_DIR_BIT | (0o755 << 16)

_FILE_BIT = (0o100000 << 16) | (0o644 << 16)


def zip_dir(dir_path: str, output_zip_path: str):
    with zipfile.ZipFile(output_zip_path, "w") as output:
        for root, _, files in os.walk(dir_path):
            if root != dir_path:
                dir_zip_path = os.path.relpath(root, dir_path)
                dir_zip_info = zipfile.ZipInfo(dir_zip_path + "/", date_time=_TIMESTAMP)
                dir_zip_info.external_attr |= _DIR_BIT
                dir_zip_info.flag_bits |= 0x800  # UTF-8 encoded file name.
                output.writestr(dir_zip_info, "", compress_type=zipfile.ZIP_STORED)

            for f in files:
                file_path = os.path.join(root, f)
                zip_path = os.path.relpath(file_path, dir_path)
                zip_info = zipfile.ZipInfo(zip_path, date_time=_TIMESTAMP)
                zip_info.flag_bits |= 0x800  # UTF-8 encoded file name.
                zip_info.external_attr |= _FILE_BIT

                with open(file_path, "rb") as f:
                    content = f.read()
                output.writestr(zip_info, content, compress_type=zipfile.ZIP_STORED)


if __name__ == "__main__":
    zip_dir(sys.argv[1], sys.argv[2])
