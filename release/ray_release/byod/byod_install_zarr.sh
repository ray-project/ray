#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

# read_zarr needs the Zarr v2 stack plus fsspec-backed cloud filesystems.
#
# zarr is pinned to 2.18.3 (the last release supporting Python 3.10, which the
# `type: gpu` / ray-ml release image forces). zarr-python 3.x is a different,
# unsupported store model. numcodecs 0.13.1 provides the default blosc/zstd
# codecs and is the matching pin for py3.10. imagecodecs provides non-stdlib codecs
# (e.g. JPEG-XL) for image arrays (UMI's camera0_rgb); 2025.3.30 is the last
# release that still ships a cp310 wheel (newer ones dropped Python 3.10). s3fs /
# gcsfs back fsspec.get_mapper() for s3:// and gs:// stores.
pip3 install --no-cache-dir \
    "zarr==2.18.3" \
    "numcodecs==0.13.1" \
    "imagecodecs==2025.3.30" \
    "fsspec" \
    "s3fs" \
    "gcsfs"
