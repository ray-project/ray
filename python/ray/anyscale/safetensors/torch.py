from typing import Dict, Optional

import torch

from ray.anyscale.safetensors._common import get_http_downloader_for_uri


def load_file(
    file_uri: str,
    *,
    device: str = "cpu",
    region: Optional[str] = None,
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
    http_downloader, url = get_http_downloader_for_uri(file_uri, region=region)
    state_dict, _ = http_downloader.restore_state_dict_from_http(
        url,
        None,
        device=device,
    )
    return state_dict
