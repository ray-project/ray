# Copyright NVIDIA Corporation 2023
# SPDX-License-Identifier: Apache-2.0

import io
import json
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray._common.utils import env_bool
from ray.data._internal.datasource_v2.formats.webdataset.webdataset_utils import (
    ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR,
    WEBDATASET_READ_CHUNK_SIZE,
    _apply_list,
    _default_decoder,
    _group_by_keys,
    _tar_file_iterator,
)
from ray.data._internal.util import iterate_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


extension_to_format = {"jpg": "jpeg"}


def _default_encoder(sample: Dict[str, Any], format: Optional[Union[str, bool]] = True):
    """A default encoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2, .jpg,
        .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl
    These are the most common extensions used in webdataset.
    For other extensions, users can provide their own encoder.

    Args:
        sample: sample to encode.
        format: optional image format hint forwarded to the underlying
            image encoder.

    Returns:
        The sample with values encoded according to their key extension.
    """
    sample = dict(sample)
    for key, value in sample.items():
        extension = key.split(".")[-1]
        if key.startswith("__"):
            continue
        elif extension in ["txt"]:
            sample[key] = value.encode("utf-8")
        elif extension in ["cls", "cls2"]:
            sample[key] = str(value).encode("utf-8")
        elif extension in ["jpg", "jpeg", "png", "ppm", "pgm", "pbm", "pnm"]:
            import numpy as np
            import PIL.Image

            if isinstance(value, np.ndarray):
                # PIL can't encode (H, W, 1) arrays.
                if value.ndim == 3 and value.shape[2] == 1:
                    value = value[:, :, 0]
                value = PIL.Image.fromarray(value)
            assert isinstance(value, PIL.Image.Image)
            stream = io.BytesIO()
            value.save(
                stream, format=extension_to_format.get(extension.lower(), extension)
            )
            sample[key] = stream.getvalue()
        elif extension == "json":
            sample[key] = json.dumps(value).encode("utf-8")
        elif extension == "npy":
            import numpy as np

            stream = io.BytesIO()
            np.save(stream, value)
            sample[key] = stream.getvalue()
        elif extension == "mp":
            import msgpack

            sample[key] = msgpack.dumps(value)
        elif extension in ["pt", "pth"]:
            import torch

            stream = io.BytesIO()
            torch.save(value, stream)
            sample[key] = stream.getvalue()
        elif extension in ["pickle", "pkl"]:
            import pickle

            stream = io.BytesIO()
            pickle.dump(value, stream)
            sample[key] = stream.getvalue()
    return sample


def _make_iterable(block: BlockAccessor):
    """Make a block iterable.

    This is a placeholder for dealing with more complex blocks.

    Args:
        block: Ray Dataset block

    Returns:
        Iterable[Dict[str,Any]]: Iterable of samples
    """
    return block.iter_rows(public_row_format=False)


class WebDatasetDatasource(FileBasedDatasource):
    """A Datasource for WebDataset datasets (tar format with naming conventions)."""

    _FILE_EXTENSIONS = ["tar"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        decoder: Optional[Union[bool, str, callable, list]] = True,
        fileselect: Optional[Union[bool, callable, list]] = None,
        filerename: Optional[Union[bool, callable, list]] = None,
        suffixes: Optional[Union[bool, callable, list]] = None,
        verbose_open: bool = False,
        expand_json: bool = False,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self.decoder = decoder
        self.fileselect = fileselect
        self.filerename = filerename
        self.suffixes = suffixes
        self.verbose_open = verbose_open
        self.expand_json = expand_json

        self._allow_unsafe_deserialization = env_bool(
            ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR, False
        )

    def _read_stream(self, stream: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read and decode samples from a stream.

        Note that fileselect selects files during reading, while suffixes
        selects files during the grouping step.

        Args:
            stream: File descriptor to read from.
            path: Path to the data.

        Yields:
            Block: Single-row blocks (one per WebDataset sample).
        """

        import pandas as pd

        def get_tar_file_iterator():
            return _tar_file_iterator(
                stream,
                fileselect=self.fileselect,
                filerename=self.filerename,
                verbose_open=self.verbose_open,
            )

        # S3 can raise transient errors during iteration
        files = iterate_with_retry(
            get_tar_file_iterator,
            "iterate tar file",
            match=self._data_context.retried_io_errors,
        )

        samples = _group_by_keys(files, meta=dict(__url__=path), suffixes=self.suffixes)
        default_decoder = partial(
            _default_decoder, allow_unsafe=self._allow_unsafe_deserialization
        )

        # Accumulate samples into chunks.
        rows = []

        for sample in samples:
            if self.decoder is not None:
                sample = _apply_list(self.decoder, sample, default=default_decoder)
            if self.expand_json:
                if isinstance(sample["json"], bytes):
                    parsed_json = json.loads(sample["json"].decode("utf-8"))
                elif isinstance(sample["json"], str):
                    parsed_json = json.loads(sample["json"])
                elif isinstance(sample["json"], dict):
                    parsed_json = sample["json"]
                else:
                    raise TypeError(
                        f"Unsupported data type" f" {type(sample['json'])} for sample"
                    )
                for k, v in parsed_json.items():
                    if k not in sample:
                        sample[k] = []
                    sample[k].append(v)
            rows.append(
                {
                    k: v[0] if isinstance(v, list) and len(v) == 1 else v
                    for k, v in sample.items()
                }
            )
            if len(rows) >= WEBDATASET_READ_CHUNK_SIZE:
                yield pd.DataFrame.from_records(rows)
                rows = []

        if len(rows) > 0:
            yield pd.DataFrame.from_records(rows)
