from typing import Dict, Optional

import torch

from ray.anyscale.safetensors._common import get_http_downloader_for_uri


def load_file(
    file_uri: str,
    *,
    device: str = "cpu",
    region: Optional[str] = None,
    # TODO(edoakes): we should support the load_model API instead of these params.
    _existing_state_dict: Optional[Dict[str, torch.Tensor]] = None,
    _strict: bool = True,
) -> Dict[str, torch.Tensor]:
    """Load a safetensors file from the given URI and return a state_dict.

    Arguments:
        uri: a remote URI specifying a safetensors file. The 'anyscale://' prefix
            can be used when running in an Anyscale cluster to access files in
            Anyscale-managed artifact storage.
        device: device to load the tensors to. Currently only "cpu" and "cuda" are
            supported. Defaults to "cpu".
        region: must be specified when loading from an 's3://' URI.

    Returns:
        A PyTorch state_dict.
    """
    http_downloader, url = get_http_downloader_for_uri(
        file_uri, region=region, strict=_strict
    )
    state_dict, _ = http_downloader.restore_state_dict_from_http(
        url,
        _existing_state_dict,
        device=device,
    )
    return state_dict
