import os
from typing import Dict, Optional, Tuple

import pycurl

from ray.anyscale.anytensor._private.http_downloader import HTTPSafetensorDownloader
from ray.anyscale.anytensor._private.logging_utils import logger
from ray.anyscale.anytensor._private.uri import parse_uri_info


class AnytensorClient:
    def __init__(
        self,
        *,
        local_cache_dir: Optional[str] = None,
        # Private arg because customers shouldn't be setting this, but it's temporarily
        # needed by RayLLM.
        _strict: bool = True,
    ):
        """Construct a client to read from the specified base directory.

        Arguments:
            local_cache_dir: a local directory that will be used to cache file
                downloads.
        """
        self._strict = _strict
        self._local_cache_dir = local_cache_dir or os.getenv("ANYTENSOR_CACHE_DIR")
        if self._local_cache_dir is not None:
            logger.info("Caching files in local directory: %s", self._local_cache_dir)

    def _get_http_downloader_for_uri(
        self, uri: str
    ) -> Tuple[HTTPSafetensorDownloader, str]:
        uri_info = parse_uri_info(uri)

        local_cache_dir = None
        if self._local_cache_dir is not None:
            # HTTPSafetensorDownloader only considers the basename when caching files,
            # so we generate a fully qualified directory name here.
            local_cache_dir = os.path.join(self._local_cache_dir, uri_info.cache_prefix)

        return (
            HTTPSafetensorDownloader(
                strict=self._strict,
                cache_directory=local_cache_dir,
                pycurl_settings={
                    pycurl.HTTPHEADER: [
                        f"{k}: {v}" for k, v in uri_info.download_headers.items()
                    ]
                },
            ),
            uri_info.download_url,
        )

    def load_state_dict(
        self,
        uri: str,
        *,
        device: str = "cpu",
        region: Optional[str] = None,
    ) -> Dict:
        """Load and return a state_dict from the given URI.

        Arguments:
            uri: a remote URI specifying a safetensors file. The 'anyscale://' prefix
                can be used when running in an Anyscale cluster to access files in
                Anyscale-managed artifact storage.
            device: device to load the tensors to. Currently "cpu" and "cuda" are
                supported. Defaults to "cpu".
            region: required for 's3://' URIs only.

        Returns:
            A PyTorch state_dict.
        """
        http_downloader, url = self._get_http_downloader_for_uri(uri)
        state_dict, _ = http_downloader.restore_state_dict_from_http(
            url, None, device=device
        )
        return state_dict

    def populate_state_dict(
        self,
        uri: str,
        *,
        state_dict: Dict,
        region: Optional[str] = None,
    ):
        """Populate the provided state_dict from the given URI.

        The tensors in the file must exactly match the provided state_dict.

        Arguments:
            uri: a remote URI specifying a safetensors file. The 'anyscale://' prefix
                can be used when running in an Anyscale cluster to access files in
                Anyscale-managed artifact storage.
            state_dict: an existing PyTorch state_dict to load the tensors to.
                The tensors in the file must exactly match the provided state_dict.
            region: required for 's3://' URIs only.

        Returns:
            None
        """
        http_downloader, url = self._get_http_downloader_for_uri(uri)
        http_downloader.restore_state_dict_from_http(
            url,
            state_dict,
        )
