import json
import os
import struct
import tempfile
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import cached_property, wraps
from typing import Any, Dict, List, Optional, Tuple, Type

from ray.anyscale.safetensors._private.lazy_torch import torch
from ray.anyscale.safetensors._private.logging_utils import logger


def cum_mean(curr_mean: float, new_val: float, n: int) -> float:
    # https://stackoverflow.com/a/37830174
    return curr_mean + (new_val - curr_mean) / n


def bytes_to_gigabytes(n: float) -> float:
    return n / 1024 / 1024 / 1024


def bytes_to_megabytes(n: float) -> float:
    return n / 1024 / 1024


@contextmanager
def get_temp_file():
    try:
        temp_file = tempfile.NamedTemporaryFile(dir="/dev/shm", delete=False)
    except Exception:
        logger.warning("Failed to create temp file in /dev/shm, falling back to /tmp")
        temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        yield temp_file
    finally:
        try:
            os.remove(temp_file.name)
        except OSError:
            pass


# Modified from bisect.py stdlib module
def _bisect_right_with_key(a, x, key, lo=0, hi=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    Modified from bisect.py stdlib module:
        key is a function that returns the key to compare with x.

    The return value i is such that all e in a[:i] have e <= x, and all e in
    a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    insert just after the rightmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if lo < 0:
        raise ValueError("lo must be non-negative")
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        # Use __lt__ to match the logic in list.sort() and in heapq
        if x < key(a[mid]):
            hi = mid
        else:
            lo = mid + 1
    return lo


def find_le(a, x, key):
    """Find rightmost index with value less than or equal to x"""
    i = _bisect_right_with_key(a, x, key)
    if i:
        return i - 1
    raise ValueError(f"No item in list is less than or equal to {x}")


# Lazily initialized to avoid importing torch.
_safetensors_dtype_to_torch_dtype: Optional[Dict[str, "torch.dtype"]] = None


def _get_safetensors_dtype_to_torch_dtype(dtype_str: str) -> "torch.dtype":
    # https://huggingface.co/docs/huggingface.js/main/en/hub/modules#dtype
    global _safetensors_dtype_to_torch_dtype

    if _safetensors_dtype_to_torch_dtype is None:
        _safetensors_dtype_to_torch_dtype = {
            "F64": torch.float64,
            "F32": torch.float32,
            "F16": torch.float16,
            "BF16": torch.bfloat16,
            "I64": torch.int64,
            "I32": torch.int32,
            "I16": torch.int16,
            "I8": torch.int8,
            "U8": torch.uint8,
            "BOOL": torch.bool,
            "F8_E4M3": torch.float8_e4m3fn,
        }

    if dtype_str not in _safetensors_dtype_to_torch_dtype:
        raise ValueError(f"dtype '{dtype_str}' is not supported")

    return _safetensors_dtype_to_torch_dtype[dtype_str]


@dataclass
class TensorMetadata:
    """
    Represents metadata for a tensor contained in a safetensors file.

    Attributes:
        data_offsets: The start and end offsets of the tensor data.
        dtype: The torch data type of the tensor.
        shape: The shape of the tensor.
    """

    data_offsets: Tuple[int, int]
    dtype: "torch.dtype"
    shape: List[int]

    @classmethod
    def from_metadata_dict(cls, dct: dict) -> "TensorMetadata":
        return cls(
            data_offsets=dct["data_offsets"],
            dtype=_get_safetensors_dtype_to_torch_dtype(dct["dtype"]),
            shape=dct["shape"],
        )


class FileSource(str, Enum):
    HTTP = "http"
    DISK = "disk"


@dataclass
class SafetensorsMetadata:
    """Represents metadata for a Safetensor.

    Attributes:
        path_or_url: The path or URL of the file.
        data_offset_bytes: The offset in bytes where the data starts in the file.
        raw_metadata: The raw metadata bytes of the file.
        metadata_header: The metadata header of the file.
        tensor_metadata: The metadata for each tensor, ordered by start offset ascending
            (for efficient lookup later).
        source: The source of the file.
    """

    path_or_url: str
    data_offset_bytes: int
    raw_metadata: bytes
    metadata_header: Optional[Dict[str, Any]]
    tensor_metadata: OrderedDict[str, TensorMetadata]
    source: FileSource

    @cached_property
    def data_size_bytes(self) -> int:
        """
        Returns the size of the data in bytes, excluding metadata.

        We take advantage of two properties of the safetensors format here:
        1. The data for each tensor is contigious.
        2. The first tensor's start offset is always 0.

        Returns:
            int: The size of the data in bytes.
        """
        return max(t.data_offsets[1] for t in self.tensor_metadata.values())

    @property
    def size_bytes(self) -> int:
        """
        Returns the total size of the Safetensor in bytes.

        Returns:
            int: The total size of the Safetensor in bytes.
        """
        return self.data_size_bytes + self.data_offset_bytes


@dataclass
class TensorRanges:
    # Range in the tensor we will be copying to
    range_in_tensor: Tuple[int, int]
    # Range in the buffer we will be copying from
    range_in_buffer: Tuple[int, int]
    # The dtype of the data in the buffer we will be copying from
    dtype: "torch.dtype"

    def __post_init__(self):
        if (
            not self.range_in_tensor[1] - self.range_in_tensor[0]
            == self.range_in_buffer[1] - self.range_in_buffer[0]
        ):
            raise ValueError("Tensor and buffer ranges must have the same size")


def get_current_torch_device() -> "torch.device":
    return torch.empty(0).device


def preprocess_safetensor_tensor_key(tensor_key: str) -> str:
    prefix = "transformer."
    if tensor_key.startswith(prefix):
        return tensor_key[len(prefix) :]
    else:
        return tensor_key


def get_safetensor_metadata_len(b: bytes) -> int:
    # See https://huggingface.co/docs/safetensors/en/metadata_parsing#usage
    return struct.unpack("<Q", b)[0]


def parse_safetensor_metadata(
    b: bytes,
) -> Tuple[Optional[Dict[str, Any]], OrderedDict[str, TensorMetadata]]:
    """
    Parses the metadata of safetensors from the given bytes.

    Args:
        b: The bytes containing the safetensor metadata.

    Returns:
        A tuple containing the metadata header and the dictionary of tensor metadata
        ordered by start offset ascending.
    """
    tensor_metadata = json.loads(b.decode("utf-8").strip())
    metadata_header = tensor_metadata.pop("__metadata__", None)
    tensor_metadata = OrderedDict(
        sorted(
            [
                (k, TensorMetadata.from_metadata_dict(v))
                for k, v in tensor_metadata.items()
            ],
            key=lambda x: x[1].data_offsets[0],
        )
    )
    return metadata_header, tensor_metadata


def retry_with_backoff(
    max_retries: int,
    initial_backoff_s: float,
    backoff_factor: float,
    exception: Tuple[Type[Exception], ...],
):
    def outer_wrapper(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            backoff = initial_backoff_s
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not isinstance(e, exception):
                        raise
                    retries += 1
                    if retries > max_retries:
                        raise
                    logger.exception(
                        "Retrying (%s/%s) %s after %s seconds...",
                        retries,
                        max_retries,
                        func,
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff *= backoff_factor

        return wrapper

    return outer_wrapper


def parse_http_headers(raw_headers: bytes) -> Dict[bytes, bytes]:
    """
    Parse HTTP headers from a buffer.

    Args:
        raw_headers: The raw HTTP headers.

    Returns:
        dict: A dictionary containing the parsed HTTP headers, where the keys are the
              header names in lowercase and the values are the header values.

    """
    lines = raw_headers.split(b"\r\n")

    headers = {}
    for line in lines:
        line = line.strip()
        if not line:
            break

        if b":" not in line:
            continue

        key, value = line.split(b":", 1)
        headers[key.lower()] = value

    return headers


def get_content_len_from_headers(headers: dict) -> int:
    """
    Get the content length from the given headers.

    Args:
        headers: The headers containing the content length information.

    Returns:
        int: The content length as an integer value.
    """
    if b"x-linked-size" in headers:
        # CloudFront missed cache
        value_bytes = headers[b"x-linked-size"]
    else:
        value_bytes = headers[b"content-length"]

    int_value = int(value_bytes.decode("utf-8").strip())
    return int_value


def get_location_from_headers(headers: dict) -> Optional[str]:
    """
    Extracts the location from the given headers.

    Args:
        headers: The headers dictionary.

    Returns:
        str or None: The location extracted from the headers, or None if not found.
    """
    location = headers.get(b"location", None)

    if location is not None:
        location = location.decode("utf-8").strip()

    return location
