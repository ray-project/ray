import atexit
import concurrent.futures
import math
import mmap
import multiprocessing as mp
import os
import pickle
import subprocess
import sys
import time
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from io import BytesIO
from multiprocessing import shared_memory
from pathlib import Path
from queue import Empty as QueueEmpty
from typing import IO, Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

import certifi
import pycurl

from ray.anyscale.safetensors._private import shm_download
from ray.anyscale.safetensors._private.base_sink import BaseSink
from ray.anyscale.safetensors._private.base_source import BaseSource, SharedMemorySource
from ray.anyscale.safetensors._private.cpu_sink import CPUSink
from ray.anyscale.safetensors._private.disk_writer import (
    DiskWriter,
    DummyDiskWriter,
    url_or_path_to_base_name,
)
from ray.anyscale.safetensors._private.env import (
    CURL_BUFFERSIZE_BYTES,
    DEFAULT_BYTES_PER_SHARD,
    DEFAULT_SINGLE_STREAM_BANDWIDTH_MB_S,
    DEFAULT_STREAMS_PER_PROCESS,
    DEFAULT_TARGET_BANDWIDTH_MB_S,
    MAX_RETRIES,
    PROCESS_SHUTDOWN_TIMEOUT_S,
)
from ray.anyscale.safetensors._private.gpu_sink import GPUSink
from ray.anyscale.safetensors._private.lazy_torch import torch
from ray.anyscale.safetensors._private.logging_utils import logger
from ray.anyscale.safetensors._private.shm_download import (
    ReadRangeInput,
    ReadRangeInputs,
    ReadRangeOutput,
)
from ray.anyscale.safetensors._private.util import (
    FileSource,
    SafetensorsMetadata,
    TensorMetadata,
    TensorRanges,
    bytes_to_gigabytes,
    bytes_to_megabytes,
    cum_mean,
    find_le,
    get_current_torch_device,
    get_location_from_headers,
    get_safetensor_metadata_len,
    get_temp_file,
    parse_http_headers,
    parse_safetensor_metadata,
    preprocess_safetensor_tensor_key,
    retry_with_backoff,
)

LOGGING_INTERVAL_S = 5
# This stores all the operations that we would like to wait on before the process exits.
# For example, when we download model weights, we want to return the model weights as
# soon as possible while writing the model weights to disk asynchronously. So, we store
# the futures for the waiting on disk writer operations in here and flush them when the
# process exits.
_PENDING_FLUSH_OPERATIONS: List[concurrent.futures.Future] = []


@atexit.register
def _flush_pending_operations(*, timeout_s: int = 300):
    global _PENDING_FLUSH_OPERATIONS
    start_time = time.perf_counter()
    while _PENDING_FLUSH_OPERATIONS:
        elapsed_time = time.perf_counter() - start_time
        if elapsed_time >= timeout_s:
            logger.warning(f"Flushing operations timed out after {timeout_s} seconds")
            break

        pending_op = _PENDING_FLUSH_OPERATIONS.pop(0)
        try:
            remaining_time = max(0, timeout_s - elapsed_time)
            pending_op.result(timeout=remaining_time)
        except concurrent.futures.TimeoutError:
            logger.warning(f"Operation timed out after {remaining_time} seconds")
        except Exception as e:
            logger.exception(f"Failed to flush pending operation: {e}")

    remaining_ops = len(_PENDING_FLUSH_OPERATIONS)
    if remaining_ops > 0:
        logger.warning(f"{remaining_ops} operations were not flushed due to timeout")


@retry_with_backoff(
    MAX_RETRIES, 0.1, 2, (pycurl.error, OSError, UnicodeDecodeError, ValueError)
)
def _get_safetensor_metadata_from_url(
    url: str, curl_handle: pycurl.Curl
) -> SafetensorsMetadata:
    logger.debug("Getting metadata for URL %s", url)
    body_buffer = BytesIO()
    headers_buffer = BytesIO()

    def _reset_buf(b: BytesIO):
        b.truncate(0)
        b.seek(0)

    def _check_status_code():
        status = curl_handle.getinfo(pycurl.HTTP_CODE)
        if status > 299:
            msg = body_buffer.getvalue().decode("utf-8")
            if status == 404:
                # NOTE(edoakes): this is temporarily a lazy import until the
                # anytensor directory is fully deprecated.
                from ray.anyscale.safetensors.exceptions import NotFoundError

                raise NotFoundError(
                    f"Fetching metadata for URL {url} returned a 404 (not found) status"
                )
            raise RuntimeError(
                f"Failed to fetch metadata with status code {status}: {msg}"
            )

    # First, fetch the length of the metadata from the first 8 bytes of the file.
    curl_handle.setopt(pycurl.URL, url)
    curl_handle.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2)
    curl_handle.setopt(pycurl.WRITEDATA, body_buffer)
    curl_handle.setopt(pycurl.HEADERFUNCTION, headers_buffer.write)
    curl_handle.setopt(pycurl.RANGE, f"{0}-{7}")

    curl_handle.perform()
    _check_status_code()

    try:
        headers = parse_http_headers(headers_buffer.getvalue())
    except Exception:
        logger.exception("Failed to parse HTTP headers")
        raise

    location = get_location_from_headers(headers)
    metadata_range = body_buffer.getvalue()
    length_of_metadata = get_safetensor_metadata_len(metadata_range)

    # Now, given the length of the metadata, fetch and parse it.
    offset = 8
    curl_handle.setopt(pycurl.RANGE, f"{offset}-{offset - 1 + length_of_metadata}")

    _reset_buf(body_buffer)
    _reset_buf(headers_buffer)
    curl_handle.perform()
    _check_status_code()

    metadata_bytes = body_buffer.getvalue()
    metadata_header, safetensors_metadata = parse_safetensor_metadata(metadata_bytes)
    return SafetensorsMetadata(
        path_or_url=location or url,
        data_offset_bytes=offset + length_of_metadata,
        raw_metadata=metadata_range + metadata_bytes,
        metadata_header=metadata_header,
        tensor_metadata=safetensors_metadata,
        source=FileSource.HTTP,
    )


def _load_safetensor_metadata_from_path(path: Path) -> SafetensorsMetadata:
    """
    Loads the metadata of safetensors from the given path.

    Args:
        path: The path to the safetensor file.

    Returns:
        A tuple containing the metadata header and the dictionary of tensor metadata
        ordered by start offset ascending.
    """
    offset = 8
    raw_metadata = BytesIO()
    with open(path, "rb") as fp:
        raw_length_of_metadata = fp.read(offset)
        length_of_metadata = get_safetensor_metadata_len(raw_length_of_metadata)
        raw_metadata.write(raw_length_of_metadata)
        metadata_bytes = fp.read(length_of_metadata)
        raw_metadata.write(metadata_bytes)
        metadata_header, safetensors_metadata = parse_safetensor_metadata(
            metadata_bytes
        )
    return SafetensorsMetadata(
        str(path.absolute()),
        offset + length_of_metadata,
        raw_metadata.getvalue(),
        metadata_header,
        safetensors_metadata,
        source=FileSource.DISK,
    )


def get_safetensor_metadata_from_urls(
    urls: Union[str, List[str]],
    pycurl_settings: Optional[Dict[int, Any]] = None,
) -> List[SafetensorsMetadata]:
    """
    Retrieves the metadata information of safetensors files from the given URLs.

    Each file has its own metadata header.
    See https://huggingface.co/docs/safetensors/en/metadata_parsing for details.

    Args:
        urls: The URLs from which to retrieve the Safetensor metadata.
        pycurl_settings: Additional settings to pass to PyCurl.

    Returns:
        A list of SafetensorMetadata objects containing the metadata information.
    """
    ret = []
    pycurl_settings = pycurl_settings or {}
    c = pycurl.Curl()
    c.setopt(pycurl.CAINFO, certifi.where())
    c.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2)
    c.setopt(pycurl.ENCODING, "")
    c.setopt(pycurl.BUFFERSIZE, CURL_BUFFERSIZE_BYTES)
    for setting, value in pycurl_settings.items():
        c.setopt(setting, value)

    for url in urls:
        ret.append(_get_safetensor_metadata_from_url(url, c))
    return ret


def start_download_in_background(
    authkey: bytes,
    payload_file: str,
) -> subprocess.Popen:
    """
    Starts a download process in the background using subprocess.Popen.

    We use a separate process to ensure it doesn't have any unnecessary dependencies
    loaded from the main process, which would slow down forking (by orders of magnitude)
    and increase memory usage.

    Essentially, this is a similar pattern to the forkserver start method in
    multiprocessing,  but unlike there, we can tightly control the loaded modules.

    Args:
        args: The arguments to be passed to the download process.
        authkey: The authentication key for the download process.
        queue: The queue to be used for communication between the main process and the
            download process.
        num_processes: The number of download processes to be started.

    Returns:
        subprocess.Popen: The Popen object representing the download process.

    """
    downloader = shm_download.__file__
    logger.debug("Payload file %s", payload_file)
    downloader_process = subprocess.Popen(
        [
            sys.executable,
            downloader,
            authkey.hex(),
            payload_file,
        ],
        env=os.environ,
    )
    return downloader_process


@contextmanager
def source_and_sink_handler(
    source: BaseSource,
    sinks: List[BaseSink],
):
    sink_shutdown_futures: List[concurrent.futures.Future] = []

    global _PENDING_FLUSH_OPERATIONS
    exception: Optional[Exception] = None
    try:
        yield source, sinks
    except Exception as e:
        exception = e
        logger.exception(f"Exception occured: {e}")
        raise e
    finally:
        for sink in sinks:
            sink_shutdown_future = sink.shutdown(
                timeout_s=PROCESS_SHUTDOWN_TIMEOUT_S, success=exception is None
            )
            if isinstance(sink_shutdown_future, concurrent.futures.Future):
                sink_shutdown_futures.append(sink_shutdown_future)

        # The source will be responsible for waiting on all the sinks to finish
        # since the source will be the last to shutdown.
        source_shutdown_future = source.shutdown(sink_shutdown_futures)
        if isinstance(source_shutdown_future, concurrent.futures.Future):
            _PENDING_FLUSH_OPERATIONS.append(source_shutdown_future)


class HTTPSafetensorDownloader:
    """
    A class for downloading and managing safetensors over HTTPS.

    This class will start multiple downloader shards (each a separate process),
    each reading a part of a safetensors file into shared memory.
    In parallel, the finished shards will be copied into GPU memory.

    Args:
        target_bandwidth_mb_s: The target bandwidth in megabytes per second. If None,
            will read from ANYTENSOR_TARGET_BANDWITH_MB_S env var.
        single_stream_bandwidth_mb_s: The bandwidth for a single stream in megabytes
            per second. If None, will read from ANYTENSOR_SINGLE_STREAM_BANDWITH_MB_S
            env var.
        max_processes: The maximum number of processes to use for downloading. Defaults
            to None (meaning number of cores on the machine obtained with os.cpu_count).
        align: Whether to align the buffers. Defaults to False.
        pycurl_settings: Additional settings to pass to PyCurl.
        cache_directory: The base directory to use for caching the downloaded files.
            If provided, the state dictionary will be restored from disk if possible,
            unless force_download=True.
        strict: Whether to fail if tensor names are mismatched between local and remote.
            Defaults to True. Ignored if existing state dict is not provided.
    """

    def __init__(
        self,
        target_bandwidth_mb_s: float = DEFAULT_TARGET_BANDWIDTH_MB_S,
        single_stream_bandwidth_mb_s: float = DEFAULT_SINGLE_STREAM_BANDWIDTH_MB_S,
        streams_per_process: int = DEFAULT_STREAMS_PER_PROCESS,
        bytes_per_shard: int = DEFAULT_BYTES_PER_SHARD,
        max_processes: Optional[int] = None,
        align: bool = False,
        pycurl_settings: Optional[Dict[int, Any]] = None,
        cache_directory: Optional[Union[str, Path]] = None,
        strict: bool = True,
    ):
        self.target_bandwidth_mb_s = target_bandwidth_mb_s
        self.single_stream_bandwidth_mb_s = single_stream_bandwidth_mb_s
        self.streams_per_process = streams_per_process
        self.bytes_per_shard = bytes_per_shard
        self.align = align
        self.max_processes = max_processes or os.cpu_count() or 1
        if self.max_processes < 1:
            raise ValueError(f"Got {max_processes=}, expected at least 1.")
        self.pycurl_settings = pycurl_settings or {}
        self.pycurl_settings.setdefault(pycurl.BUFFERSIZE, CURL_BUFFERSIZE_BYTES)
        self.cache_directory = (
            Path(str(os.path.expandvars(cache_directory)))
            if cache_directory is not None
            else None
        )
        self.strict = strict

    def _get_device_sink(
        self,
        buf: memoryview,
        device: "torch.device",
        state_dict: Dict[str, "torch.Tensor"],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
    ) -> BaseSink:
        if device.type == "cuda":
            return GPUSink(
                buf,
                state_dict,
                urls_to_ranges_to_tensors,
                max_read_range_size=self.bytes_per_shard,
            )
        elif device.type == "cpu":
            return CPUSink(
                buf,
                state_dict,
                urls_to_ranges_to_tensors,
            )
        else:
            raise ValueError(f"Unsupported device type: {device.type}")

    def _get_disk_writer(
        self,
        shm: shared_memory.SharedMemory,
        metadatas: List[SafetensorsMetadata],
        async_flush: bool = False,
    ) -> BaseSink:
        disk_writer: BaseSink = (
            DiskWriter(
                self.cache_directory,
                shm.buf,
                metadatas,
                # We will start manually later, so that we can
                # overlap with downloader startup
                start_on_enter=False,
                async_flush=async_flush,
            )
            if self.cache_directory
            else DummyDiskWriter()
        )

        return disk_writer

    def _validate_tensor_names(
        self, local_tensor_names: Set[str], remote_tensor_names: Set[str]
    ):
        tensors_present_in_local_but_not_remote = local_tensor_names.difference(
            remote_tensor_names
        )
        tensors_present_in_remote_but_not_local = remote_tensor_names.difference(
            local_tensor_names
        )
        if (
            tensors_present_in_local_but_not_remote
            or tensors_present_in_remote_but_not_local
        ):
            msg = (
                "Mismatch between remote and local state dict tensor names. "
                "Tensors missing in local: "
                f"{tensors_present_in_remote_but_not_local}. "
                "Tensors missing in remote: "
                f"{tensors_present_in_local_but_not_remote}"
            )
            if self.strict:
                raise ValueError(msg)
            else:
                logger.warning(msg)

    def _create_state_dict_or_validate_tensors_with_remote(
        self,
        state_dict: Optional[Dict[str, "torch.Tensor"]],
        remote_tensor_metadata: Dict[str, TensorMetadata],
        all_tensor_names_in_remote: Set[str],
        should_create_state_dict: bool,
        device: Optional[Union[str, "torch.device"]] = None,
    ) -> Dict[str, "torch.Tensor"]:
        state_dict = state_dict if state_dict is not None else {}
        for tensor_name, tensor_metadata in remote_tensor_metadata.items():
            all_tensor_names_in_remote.add(tensor_name)
            tensor_name = preprocess_safetensor_tensor_key(tensor_name)
            if should_create_state_dict:
                state_dict[tensor_name] = torch.empty(
                    tensor_metadata.shape,
                    dtype=tensor_metadata.dtype,
                    device=device,
                )
            elif tensor_name in state_dict:
                self._validate_tensor_in_state_dict(
                    tensor_name,
                    state_dict[tensor_name],
                    tensor_metadata.shape,
                    tensor_metadata.dtype,
                )

        return state_dict

    def _generate_shard_ranges_to_tensor_ranges_map_for_url(
        self, metadata: SafetensorsMetadata
    ) -> Dict[Tuple[int, int], Dict[str, TensorRanges]]:
        shard_ranges = self._generate_shard_ranges(
            metadata.data_size_bytes,
            self.bytes_per_shard,
        )
        return self._map_shard_ranges_to_tensors(shard_ranges, metadata.tensor_metadata)

    def restore_state_dict_from_http(
        self,
        url: Union[str, Sequence[str]],
        state_dict: Optional[Dict[str, "torch.Tensor"]] = None,
        device: Optional[Union[str, "torch.device"]] = None,
        force_download: bool = False,
    ) -> Tuple[Dict[str, "torch.Tensor"], Dict[str, SafetensorsMetadata]]:
        """
        Restores the state dictionary from HTTP(s) URLs to safetensor files.

        Args:
            url: The URL(s) to safetensor files from which to restore the tensors.
            state_dict: The state dictionary to use. If provided, only tensors present
                in the state dictionary will be restored in-place. If None, a new state
                dictionary will be created with all tensors present in
                the downloaded files. If empty, tensors will be created in-place.
            device: The device on which to create the tensors. If None, the current
                torch device will be used. Ignored if state_dict is provided.
            force_download: Whether to force downloading the files even if they are
                present in the base directory. Defaults to False.

        Returns:
            A tuple containing the state dictionary and a dict of
            URL:SafetensorsMetadata objects for the downloaded or restored files.
        """
        metadata_start_t = time.perf_counter()
        if isinstance(url, str):
            url = [url]

        if not url:
            raise ValueError("No URLs provided")

        # deduplicate
        url = list(set(url))

        should_create_state_dict = not bool(state_dict)
        state_dict = state_dict if state_dict is not None else {}
        device = torch.device(device) if device else get_current_torch_device()

        all_tensor_names_in_remote = set()
        all_metadatas = {}

        if self.cache_directory is not None:
            # Note: This will block until load is complete. In case of
            # partial files being present, it is suboptimal. However,
            # that should be a rare occurence.
            if not force_download:
                (
                    restored_urls_and_metadata,
                    disk_tensor_names,
                ) = self._maybe_restore_state_dict_from_local_cache(
                    self.cache_directory, url, state_dict, device=device
                )
                all_metadatas.update(restored_urls_and_metadata)
                all_tensor_names_in_remote.update(disk_tensor_names)
                remaining_urls = [u for u in url if u not in restored_urls_and_metadata]
                if not remaining_urls:
                    self._validate_tensor_names(
                        set(state_dict), all_tensor_names_in_remote
                    )
                    return state_dict, all_metadatas
                url = remaining_urls

        metadatas = get_safetensor_metadata_from_urls(url, self.pycurl_settings)
        size_bytes = sum(metadata.data_size_bytes for metadata in metadatas)
        logger.info(
            "Got %s file%s to download (%.2f GB total)",
            len(metadatas),
            "s" if len(metadatas) > 1 else "",
            bytes_to_gigabytes(size_bytes),
        )

        urls_to_ranges_to_tensors = {}
        urls_to_offsets = {}
        offset = 0
        num_shards = math.ceil(
            self.target_bandwidth_mb_s / self.single_stream_bandwidth_mb_s
        )

        for metadata in metadatas:
            urls_to_ranges_to_tensors[
                metadata.path_or_url
            ] = self._generate_shard_ranges_to_tensor_ranges_map_for_url(metadata)

            state_dict = self._create_state_dict_or_validate_tensors_with_remote(
                state_dict,
                metadata.tensor_metadata,
                all_tensor_names_in_remote,
                should_create_state_dict,
                device=device,
            )
            # We want to know where the data for each URL starts in the shared memory
            urls_to_offsets[metadata.path_or_url] = (
                offset,
                metadata.data_offset_bytes,
            )
            offset += metadata.data_size_bytes

        self._validate_tensor_names(set(state_dict), all_tensor_names_in_remote)

        read_http_start_t = time.perf_counter()
        dur_s = read_http_start_t - metadata_start_t
        logger.debug("Finished loading & processing metadata in %.2fs", dur_s)

        source = SharedMemorySource(size_bytes, async_shutdown=True)
        disk_writer = self._get_disk_writer(source.shm, metadatas, async_flush=True)
        device_sink = self._get_device_sink(
            source.shm.buf,
            device,
            state_dict,
            urls_to_ranges_to_tensors,
        )

        sinks = [disk_writer, device_sink]

        with source_and_sink_handler(source, sinks) as (source, sinks):
            with get_temp_file() as payload_file:
                self._read_http_into_buffers(
                    source.shm,
                    sinks,
                    urls_to_ranges_to_tensors,
                    urls_to_offsets,
                    size_bytes,
                    payload_file,
                    num_shards=num_shards,
                )
        read_http_end_t = time.perf_counter()
        dur_s = read_http_end_t - read_http_start_t
        logger.info(
            "Finished download in %.2fs (%.2f GB/s)",
            dur_s,
            bytes_to_gigabytes(size_bytes) / dur_s,
        )
        all_metadatas.update({metadata.path_or_url: metadata for metadata in metadatas})
        return state_dict, all_metadatas

    def _validate_tensor_in_state_dict(
        self,
        tensor_name: str,
        local_tensor: "torch.Tensor",
        remote_shape: Sequence[int],
        remote_dtype: "torch.dtype",
    ):
        local_shape = local_tensor.shape
        local_dtype = local_tensor.dtype

        if not local_tensor.is_contiguous():
            raise ValueError(
                f"'{tensor_name}' is not contiguous. All tensors passed "
                "in a state_dict to populate must be contiguous and dense."
            )

        # We allow bigger tensors due to eg. padding
        if not all(local_shape[i] >= remote_shape[i] for i in range(len(remote_shape))):
            raise ValueError(
                f"Shape mismatch for {tensor_name}! local state dict {local_shape} "
                f"vs remote {remote_shape} (every element in local must be >= remote)"
            )
        if local_dtype != remote_dtype:
            raise ValueError(
                f"Dtype mismatch for {tensor_name}! local state dict {local_dtype} "
                f"vs remote {remote_dtype}"
            )

    def _generate_shard_ranges(
        self, size_bytes: int, bytes_per_shard: int
    ) -> List[Tuple[int, int]]:
        """
        Generates a list of ranges representing the start and end indices of each shard.
        Each shard will be downloaded in parallel by a separate process.

        Args:
            size_bytes: The size of the data in bytes.
            bytes_per_shard: The number of bytes per shard.

        Returns:
            A list of tuples representing the start and end indices of each shard.

        """
        ranges = []

        start_index = 0
        end_index = bytes_per_shard
        done = False
        while not done:
            if end_index > size_bytes:
                end_index = size_bytes
                done = True

            new_range = (start_index, end_index)
            if start_index < end_index:
                ranges.append(new_range)

            start_index = end_index
            end_index += bytes_per_shard

            if start_index > size_bytes:
                break

        return ranges

    def _map_shard_ranges_to_tensors(
        self,
        shard_ranges: List[Tuple[int, int]],
        tensor_metadata: OrderedDict[str, TensorMetadata],
    ) -> Dict[Tuple[int, int], Dict[str, TensorRanges]]:
        """Returns a dictionary mapping shard ranges to tensors.

        We need to map the shard ranges to the tensors they contain. This is because the
        data for each tensor may be spread across multiple shards. This method will
        return a dictionary where the keys are the shard ranges and the values are
        dictionaries mapping tensor names to TensorRanges objects.

        Args:
            shard_ranges: A list of ranges represented as (start_idx, end_idx).
            tensor_metadata: A dictionary containing metadata for each tensor.

        Returns:
            A dictionary where the keys are ranges and the values are dictionaries
            mapping tensor names to TensorRanges objects.
        """
        shard_ranges_to_tensors_map: Dict[
            Tuple[int, int], Dict[str, TensorRanges]
        ] = defaultdict(dict)

        for tensor_name, metadata in tensor_metadata.items():
            whole_data_offsets = metadata.data_offsets
            tensor_in_whole_data_start_offset = whole_data_offsets[0]
            tensor_in_whole_data_end_offset = whole_data_offsets[1]
            tensor_size = (
                tensor_in_whole_data_end_offset - tensor_in_whole_data_start_offset
            )
            tensor_range_end = 0
            # tensor_in_whole_data_start_offset refers to the start of the tensor in
            # the whole data
            # We need to find the shard range that contains this offset
            # Example 1:
            #   shard_ranges = [(0, 100), (100, 200), (200, 300)]
            #   tensor_in_whole_data_start_offset = 50
            #   The tensor starts in the shard at index=0 (first shard), because 50
            #   is between 0 and 100
            # Example 2:
            #   shard_ranges = [(0, 100), (100, 200), (200, 300)]
            #   tensor_in_whole_data_start_offset = 100
            #   The tensor starts in the shard at index=1 (second shard), because 100
            #   is between 100 and 200
            shard_idx = find_le(
                shard_ranges, tensor_in_whole_data_start_offset, key=lambda x: x[0]
            )
            shard_range = shard_ranges[shard_idx]
            offset = tensor_in_whole_data_start_offset - shard_range[0]
            assert offset >= 0

            while tensor_size > 0:
                shard_range_start, shard_range_end = shard_ranges[shard_idx]
                shard_range_size = shard_range_end - shard_range_start
                tensor_range_start = tensor_range_end
                # How much of the tensor is present in this shard
                tensor_size_this_buffer = min(tensor_size, shard_range_size - offset)

                if tensor_size_this_buffer == 0:
                    shard_idx += 1
                    offset = 0
                    continue

                tensor_range_end = tensor_range_start + tensor_size_this_buffer
                assert tensor_range_end > tensor_range_start
                assert offset + tensor_size_this_buffer <= shard_range_size

                shard_ranges_to_tensors_map[(shard_range_start, shard_range_end)][
                    tensor_name
                ] = TensorRanges(
                    range_in_tensor=(tensor_range_start, tensor_range_end),
                    range_in_buffer=(offset, offset + tensor_size_this_buffer),
                    dtype=metadata.dtype,
                )

                # Update the tensor size and offset
                tensor_size -= tensor_size_this_buffer
                # We have a guarantee that the rest of the tensor
                # will be in next shard
                offset += tensor_size_this_buffer
                if offset == shard_range_size:
                    shard_idx += 1
                    offset = 0

        return shard_ranges_to_tensors_map

    def _read_http_into_buffers(
        self,
        shm: shared_memory.SharedMemory,
        device_sinks: List[BaseSink],
        urls_to_ranges_to_tensors: Dict[
            str, Dict[Tuple[int, int], Dict[str, TensorRanges]]
        ],
        urls_to_offsets: Dict[str, Tuple[int, int]],
        data_size_bytes: int,
        payload_file: IO,
        num_shards: int,
    ):
        """Reads data from HTTP URLs into shared memory buffers.

        Args:
            state_dict: A dictionary containing the state of the model.
            urls_to_ranges_to_tensors: A dictionary mapping URLs to dictionaries that
                map ranges to tensors.
            urls_to_offsets: A dictionary mapping URLs to offsets.
            data_size_bytes: Total size of data to download.
            payload_file: The payload file pointer.

        Raises:
            subprocess.CalledProcessError: If the subprocess returns a non-zero exit
                code.

        Returns:
            None
        """
        # We need a flat list to pass to subprocesses
        flat_ranges_to_tensors_map = []
        for url, ranges_to_tensors in urls_to_ranges_to_tensors.items():
            for r, range_to_tensors in ranges_to_tensors.items():
                flat_ranges_to_tensors_map.append((r, range_to_tensors, url))

        # How many streams we will open.
        num_shards = min(num_shards, len(flat_ranges_to_tensors_map))

        # For statistics
        finished_shards = 0
        bytes_downloaded = 0
        shard_min_bytes_per_s = None
        shard_max_bytes_per_s = None
        shard_mean_bytes_per_s = None
        start_timestamp = time.monotonic()
        logging_timestamp = start_timestamp

        with mp.Manager() as manager:
            # Queue holding which downloader shards have finished
            queue = manager.Queue()
            # Args to pass to the downloader shards
            args_bytes = ReadRangeInputs(
                inputs=[
                    ReadRangeInput(
                        shm_name=shm.name,
                        start_offset=r[0],
                        end_offset=r[1],
                        url_offsets=urls_to_offsets[url],
                        url=url,
                    )
                    for r, _, url in flat_ranges_to_tensors_map
                ],
                pycurl_settings=self.pycurl_settings,
                max_retries=MAX_RETRIES,
            ).to_bytes()

            with payload_file as f:
                pickle.dump(
                    (
                        args_bytes,
                        queue,
                        num_shards,
                        self.streams_per_process,
                        self.max_processes,
                    ),
                    f,
                    protocol=pickle.HIGHEST_PROTOCOL,
                )

            # Start the downloader coordinator, which will spawn
            # downloader shards.
            download_process = start_download_in_background(
                mp.current_process().authkey, payload_file.name
            )

            t = time.perf_counter()

            # While downloader starts, init the pinned buffers
            # and disk writer in parallel.
            for device_sink in device_sinks:
                device_sink.start()

            et = time.perf_counter()
            logger.debug("Preparing buffers took %.2fs", et - t)

            shard_output: Optional[Union[Exception, bytes]] = None
            # While the downloader process is alive...
            while True:
                # Try to get an item from the queue
                try:
                    shard_output = queue.get(timeout=0.1)
                except QueueEmpty:
                    # If the queue is empty, check if the process is done
                    if (
                        download_process.poll() is not None
                        and download_process.returncode != 0
                    ):
                        # We got an error, break immediately
                        break
                    # Otherwise, continue to get item from queue again
                    continue

                # if we are succesfull the last item in queue must be None
                if shard_output is None:
                    break

                # If the output is an exception, raise it
                # Note that the download process should stop on first
                # exception by itself, but we terminate it just to be sure
                if isinstance(shard_output, Exception):
                    download_process.terminate()
                    download_process.wait(timeout=PROCESS_SHUTDOWN_TIMEOUT_S)
                    raise shard_output

                # Unpack the output
                output = ReadRangeOutput.from_bytes(shard_output)

                # Update statistics
                finished_shards += 1
                shard_bytes_downloaded = output.data_end - output.data_start
                bytes_downloaded += shard_bytes_downloaded
                shard_bytes_per_s = shard_bytes_downloaded / output.time_s
                if (
                    shard_min_bytes_per_s is None
                    or shard_bytes_per_s < shard_min_bytes_per_s
                ):
                    shard_min_bytes_per_s = shard_bytes_per_s
                if (
                    shard_max_bytes_per_s is None
                    or shard_bytes_per_s > shard_max_bytes_per_s
                ):
                    shard_max_bytes_per_s = shard_bytes_per_s
                if shard_mean_bytes_per_s is None:
                    shard_mean_bytes_per_s = shard_bytes_per_s
                else:
                    shard_mean_bytes_per_s = cum_mean(
                        shard_mean_bytes_per_s, shard_bytes_per_s, finished_shards
                    )

                # Add the data to the GPU sink and disk writer.
                for device_sink in device_sinks:
                    device_sink.process_output(output)

                new_logging_timestamp = time.monotonic()
                if (
                    new_logging_timestamp - logging_timestamp >= LOGGING_INTERVAL_S
                    or bytes_downloaded == data_size_bytes
                ):
                    logging_timestamp = new_logging_timestamp
                    dur_s = time.monotonic() - start_timestamp
                    gb_downloaded = bytes_to_gigabytes(bytes_downloaded)
                    logger.info(
                        "Downloaded %.2f/%.2f GB (%.1f%%, %.2f GB/s)",
                        gb_downloaded,
                        bytes_to_gigabytes(data_size_bytes),
                        (bytes_downloaded / data_size_bytes) * 100,
                        (gb_downloaded / dur_s),
                    )
                    logger.debug(
                        "Per shard stats: min MB/s %.6f, max MB/s %.6f, mean MB/s %.6f",
                        bytes_to_megabytes(shard_min_bytes_per_s),
                        bytes_to_megabytes(shard_max_bytes_per_s),
                        bytes_to_megabytes(shard_mean_bytes_per_s),
                    )
            # Wait for the download process to finish
            download_process.wait(timeout=PROCESS_SHUTDOWN_TIMEOUT_S)
            # Last shard_output must be None if we have finished
            # succesfully
            if download_process.returncode not in (None, 0):
                raise RuntimeError(
                    "Failed to download tensors."
                ) from subprocess.CalledProcessError(
                    download_process.returncode, download_process.args
                )

        if bytes_downloaded != data_size_bytes:
            raise RuntimeError(
                f"{bytes_downloaded} didn't match {data_size_bytes}. "
                "This points to an incomplete download. Try retrying."
            )

    def _load_single_safetensors_file_from_disk(
        self,
        path: Path,
        state_dict: Dict[str, "torch.Tensor"],
        should_create_tensors: bool,
        device: "torch.device",
    ) -> Tuple[SafetensorsMetadata, Set[str]]:
        """Loads a single safetensors file from disk.

        Args:
            path: The path to the safetensors file.
            state_dict: The state dictionary to populate with loaded tensors.
            should_create_tensors: Whether to create new tensors in state dict
                or copy data to existing tensors.
            device: The device to load the tensors to.

        Returns:
            A tuple containing the metadata and the names of the tensors loaded.
        """
        if not path.exists():
            raise FileNotFoundError(f"File {path} does not exist.")
        if not path.suffix == ".safetensors":
            raise ValueError(f"File {path} is not a safetensors file.")

        logger.info("Restoring from disk")
        metadata = _load_safetensor_metadata_from_path(path)
        file_descriptor = os.open(path, os.O_RDWR)
        mmap_file = mmap.mmap(file_descriptor, 0)
        buf = memoryview(mmap_file)

        urls_to_ranges_to_tensors = {}
        disk_tensor_names: Set[str] = set()
        urls_to_ranges_to_tensors[
            metadata.path_or_url
        ] = self._generate_shard_ranges_to_tensor_ranges_map_for_url(metadata)
        state_dict = self._create_state_dict_or_validate_tensors_with_remote(
            state_dict,
            metadata.tensor_metadata,
            disk_tensor_names,
            should_create_tensors,
            device=device,
        )

        with self._get_device_sink(
            buf,
            device,
            state_dict,
            urls_to_ranges_to_tensors,
        ) as device_sink:
            for r, _ in urls_to_ranges_to_tensors[metadata.path_or_url].items():
                # We mmap'd the entire file which includes the metadata. This means we
                # will need to start reading the tensor data starting from the metadata
                # offset.
                read_range_output = ReadRangeOutput(
                    data_start=metadata.data_offset_bytes + r[0],
                    data_end=metadata.data_offset_bytes + r[1],
                    logical_start=r[0],
                    logical_end=r[1],
                    url=metadata.path_or_url,
                    time_s=0.0,  # Placeholder, reading from disk
                )

                device_sink.process_output(read_range_output)

        return metadata, disk_tensor_names

    def _maybe_restore_state_dict_from_local_cache(
        self,
        cache_directory: Path,
        urls: List[str],
        state_dict: Dict[str, "torch.Tensor"],
        device: Optional[Union[str, "torch.device"]] = None,
    ) -> Tuple[Dict[str, SafetensorsMetadata], Set[str]]:
        """
        Restores the state dictionary from  local disk cache if the corresponding
        files exist.

        Args:
            cache_directory: The base directory where the files are located.
            urls: The list of URLs.
            state_dict: The state dictionary to be restored.
            device: The device on which to create the tensors. If None, the current
                torch device will be used. Ignored if non-empty state_dict is provided.

        Returns:
            Tuple of metadata for the restored files and names of tensors present.

        """
        paths_to_urls = {
            cache_directory / url_or_path_to_base_name(url): url for url in urls
        }
        paths_that_exist_on_disk = tuple(
            path for path in paths_to_urls.keys() if path.exists()
        )
        _, metadata, disk_tensor_names = self._restore_state_dict_from_disk(
            paths_that_exist_on_disk, state_dict, device=device
        )
        metadata = {
            paths_to_urls[path]: metadata[str(path)]
            for path in paths_that_exist_on_disk
        }
        return metadata, disk_tensor_names

    def restore_state_dict_from_disk(
        self,
        paths: Iterable[Union[str, Path]],
        state_dict: Optional[Dict[str, "torch.Tensor"]] = None,
        device: Optional[Union[str, "torch.device"]] = None,
    ) -> Tuple[Dict[str, "torch.Tensor"], Dict[str, SafetensorsMetadata], Set[str]]:
        """Restores the state dictionary from safetensors files on disk.

        Args:
            paths: Safetensors file paths from which to load the
                state dictionary.
            state_dict: The state dictionary to use.
                If provided, only tensors present in the state dictionary will be
                restored in-place.
                If None, a new state dictionary will be created with all tensors present
                in the downloaded files. If empty, tensors will be created in-place.
            device: The device on which to create the tensors. If None, the current
                torch device will be used. Ignored if non-empty state_dict is provided.

        Returns:
            Tuple of:
                Dict[str, torch.Tensor]: The restored state dictionary.
                Dict[str, SafetensorsMetadata]: URL to metadata of the restored files.
                Set[str]: Names of tensors present in local files.
        """
        return self._restore_state_dict_from_disk(
            [Path(p) for p in paths], state_dict=state_dict, device=device
        )

    def _restore_state_dict_from_disk(
        self,
        paths: Iterable[Path],
        state_dict: Optional[Dict[str, "torch.Tensor"]] = None,
        device: Optional[Union[str, "torch.device"]] = None,
    ) -> Tuple[Dict[str, "torch.Tensor"], Dict[str, SafetensorsMetadata], Set[str]]:
        should_create_tensors = not bool(state_dict)
        state_dict = state_dict if state_dict is not None else {}
        device = torch.device(device) if device else get_current_torch_device()
        disk_tensor_names = set()
        all_metadata = {}
        for path in paths:
            metadata, tensor_names = self._load_single_safetensors_file_from_disk(
                path,
                state_dict,
                should_create_tensors,
                device,
            )
            all_metadata[str(path)] = metadata
            disk_tensor_names.update(tensor_names)
        return state_dict, all_metadata, disk_tensor_names
