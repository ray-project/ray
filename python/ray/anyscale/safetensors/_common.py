import os
from typing import Optional, Tuple

import pycurl

from ray.anyscale.safetensors._private.http_downloader import HTTPSafetensorDownloader
from ray.anyscale.safetensors._private.uri import parse_uri_info

_LOCAL_CACHE_DIR: Optional[str] = os.environ.get(
    "ANYSCALE_SAFETENSORS_LOCAL_CACHE_DIR", None
)


def set_local_cache_dir(local_cache_dir: Optional[str]):
    """Set the directory to be used as a local cache by load commands.

    Setting `None` disables local caching.
    """
    global _LOCAL_CACHE_DIR
    _LOCAL_CACHE_DIR = local_cache_dir


def get_local_cache_dir() -> Optional[str]:
    """Get the directory to be used as a local cache by load commands."""
    return _LOCAL_CACHE_DIR


def get_http_downloader_for_uri(
    uri: str, *, region: Optional[str], strict: bool = True
) -> Tuple[HTTPSafetensorDownloader, str]:
    uri_info = parse_uri_info(uri, region=region)

    local_cache_dir = get_local_cache_dir()
    if local_cache_dir:
        # HTTPSafetensorDownloader only considers the basename when caching files,
        # so we generate a fully qualified directory name here.
        local_cache_dir = os.path.join(local_cache_dir, uri_info.cache_prefix)

    return (
        HTTPSafetensorDownloader(
            strict=strict,
            cache_directory=local_cache_dir,
            pycurl_settings={
                pycurl.HTTPHEADER: [
                    f"{k}: {v}" for k, v in uri_info.download_headers.items()
                ]
            },
        ),
        uri_info.download_url,
    )
