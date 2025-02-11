import gc
import math
import multiprocessing as mp
import os
import pickle
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type
from unittest.mock import patch

from ray.anyscale.safetensors._private.logging_utils import logger

if TYPE_CHECKING:
    import pycurl

DEFAULT_MAX_RETRIES = 5


def noop(*args, **kwargs):
    pass


@dataclass
class ReadRangeInput:  # type: ignore
    shm_name: str

    # The start byte offset we want to read from the file.
    start_offset: int
    # The end byte offset we want to read from the file.
    end_offset: int

    # The first element is the offset from the start index in the buffer.
    # So, when we write to the buffer, we start at start_offset + url_offsets[0]
    # The second element is the offset from the start index when making the request.
    # So, when we make the ranged request, we start at start_offset + url_offsets[1]
    url_offsets: Tuple[int, int]
    url: str
    pycurl_settings: Optional[Dict[int, Any]] = None
    max_retries: Optional[int] = None

    def to_bytes(self) -> bytes:
        return pickle.dumps(asdict(self))

    @classmethod
    def from_bytes(cls, b: bytes) -> "ReadRangeInput":
        return cls(**pickle.loads(b))


@dataclass
class ReadRangeInputs:  # type: ignore
    inputs: List[ReadRangeInput]
    pycurl_settings: Optional[Dict[int, Any]] = None
    max_retries: Optional[int] = None

    def to_bytes(self) -> bytes:
        return pickle.dumps(asdict(self))

    @classmethod
    def from_bytes(cls, b: bytes) -> "ReadRangeInputs":
        d: Dict = pickle.loads(b)
        return cls(
            inputs=[ReadRangeInput(**inp) for inp in d.pop("inputs")],
            **d,
        )


@dataclass
class ReadRangeOutput:  # type: ignore
    # The start byte we want to write to the shared buffer
    data_start: int
    # The end byte we want to write to the shared buffer
    data_end: int
    # The start byte we want to read from the file
    logical_start: int
    # The end byte we want to read from the file
    logical_end: int
    url: str
    time_s: float

    def to_bytes(self) -> bytes:
        return pickle.dumps(asdict(self))

    @classmethod
    def from_bytes(cls, b: bytes) -> "ReadRangeOutput":
        return cls(**pickle.loads(b))


class BufferFileWrapper:
    """
    A simple wrapper around a buffer to provide a file-like API.

    Ensures we do not make unncessary copies and lose "sharedness" of the buffer.
    """

    def __init__(self, buffer: memoryview):
        self.memview = buffer
        self.offset = 0
        self.upper_bound = len(buffer)

    def set_upper_bound(self, upper_bound: int):
        self.upper_bound = upper_bound

    def seek(self, offset: int):
        self.offset = offset

    def _write(self, data: bytes, start: int, end: int):
        # truncate data if it exceeds the bound
        # this may happen when we have an error returned from server
        if end > self.upper_bound:
            logger.warning(
                "Buffer truncating data len %s to "
                "%s. This should only happen in case "
                "of a server error!",
                len(data),
                self.upper_bound - start,
            )
            end = self.upper_bound
            data = data[: end - start]
        self.memview[start:end] = data

    def write(self, data: bytes):
        old_offset = self.offset
        new_offset = self.offset + len(data)

        try:
            self._write(data, old_offset, new_offset)
        except Exception:
            logger.exception(
                "Buffer error: old_offset %s, " "new_offset %s, write_size %s",
                old_offset,
                new_offset,
                len(data),
            )
            return -1

        self.offset = new_offset

        return len(data)


class CurlHandleFactory:
    @classmethod
    def init_handle(cls, pycurl_settings: Dict[int, Any]) -> "pycurl.Curl":
        import certifi
        import pycurl

        handle = pycurl.Curl()
        handle.setopt(pycurl.CAINFO, certifi.where())
        handle.setopt(pycurl.ENCODING, "")
        # handle.setopt(pycurl.NOSIGNAL, 1)
        handle.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2)
        handle.setopt(pycurl.CONNECTTIMEOUT, 30)
        handle.setopt(pycurl.TIMEOUT, 180)
        # handle.setopt(pycurl.VERBOSE, True)
        for setting, value in pycurl_settings.items():
            handle.setopt(setting, value)
        return handle

    @classmethod
    def prepare_handle(
        cls, handle, *, input: ReadRangeInput, io: BufferFileWrapper
    ) -> "pycurl.Curl":
        start_index = input.start_offset
        end_index = input.end_offset
        buffer_start_index, data_start_index = input.url_offsets
        url = input.url

        start_offset = start_index + buffer_start_index
        data_range_start = start_index + data_start_index
        data_range_end = end_index + data_start_index - 1
        bytes_to_download = end_index - start_index
        io.seek(start_offset)
        io.set_upper_bound(start_offset + bytes_to_download)

        handle.setopt(handle.URL, url)
        handle.setopt(handle.RANGE, f"{data_range_start}-{data_range_end}")
        handle.setopt(handle.WRITEDATA, io)

        handle.url = url
        handle.retries = 0
        handle.start_offset = start_offset
        handle.initial_start_range = data_range_start
        handle.logical_range = (start_index, end_index)
        handle.range = (data_range_start, data_range_end)
        handle.io = io

        return handle


class MultiCurlHandle:
    def __init__(
        self,
        *,
        on_success: Callable[["MultiCurlHandle", "pycurl.Curl"], None],
        on_failure: Callable[["MultiCurlHandle", "pycurl.Curl", int, str], None],
        handle_factory: Type[CurlHandleFactory] = CurlHandleFactory,
        io_cls: Type[BufferFileWrapper] = BufferFileWrapper,
    ) -> None:
        self.free_handles: List["pycurl.Curl"] = []
        self.handles: List["pycurl.Curl"] = []
        self.handle_factory = handle_factory
        self.io_cls = io_cls
        self._multi_handle: Optional["pycurl.CurlMulti"] = None
        self._on_success = on_success
        self._on_failure = on_failure
        self._pid = os.getpid()

    @property
    def multi_handle(self) -> "pycurl.CurlMulti":
        import pycurl

        # Lazily initialized multi handle.
        if self._multi_handle is None:
            self._multi_handle = pycurl.CurlMulti()
            self._multi_handle.setopt(pycurl.M_PIPELINING, 2)

        return self._multi_handle

    def prepare(
        self,
        num_handles: int,
        inputs: List[ReadRangeInput],
        buffer: memoryview,
        pycurl_settings: Dict[int, Any],
    ):
        self.inputs = inputs
        self.buffer = buffer
        for _i in range(num_handles):
            handle = self.handle_factory.init_handle(pycurl_settings)
            self.free_handles.append(handle)
            self.handles.append(handle)

    def _perform_iteration(self):
        import pycurl

        num_handles = None
        while True:
            ret, num_handles = self.multi_handle.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        return num_handles

    def _process_iteration(self, num_processed: int):
        while True:
            num_q, ok_list, err_list = self.multi_handle.info_read()
            for handle in ok_list:
                self.multi_handle.remove_handle(handle)
                self._on_success(self, handle)
                self.free_handles.append(handle)
            for handle, errno, errmsg in err_list:
                self._on_failure(self, handle, errno, errmsg)
            num_processed = num_processed + len(ok_list)
            if num_q == 0:
                break
        return num_processed

    def _prepare_iteration(self):
        while self.inputs and self.free_handles:
            input = self.inputs.pop(0)
            handle = self.free_handles.pop()
            wrapper = self.io_cls(self.buffer)
            handle = self.handle_factory.prepare_handle(
                handle,
                input=input,
                io=wrapper,
            )
            self.multi_handle.add_handle(handle)

    def perform(self):
        self._start_time = time.perf_counter()
        num_urls = len(self.inputs)
        num_processed = 0
        while num_processed < num_urls:
            self._prepare_iteration()
            self._perform_iteration()
            num_processed = self._process_iteration(num_processed)
            self.multi_handle.select(1.0)
            time.sleep(0)

    def cleanup(self):
        for handle in self.handles:
            handle.close()
        self.free_handles.clear()
        self.handles.clear()
        if self.multi_handle:
            self.multi_handle.close()
            self._multi_handle = None

    def __del__(self):
        self.cleanup()


def _worker_wrapper(i: int, out_queue: mp.Queue, callable: Callable, *args, **kwargs):
    ret = callable(*args, **kwargs)
    out_queue.put((i, ret))
    out_queue.close()
    out_queue.join_thread()


def forked_run(
    callable: Callable,
    args: ReadRangeInputs,
    queue: mp.Queue,
    num_streams: int,
    streams_per_process: int,
    max_processes: int,
):
    """
    Run the given callable function in parallel using multiple processes.

    Number of spawned processes will equal math.ceil(num_streams / streams_per_process).

    Args:
        callable: The function to be executed in parallel.
        args: The arguments to be passed to the callable function.
        queue: The queue to store the results of the callable function.
        num_streams: The number of streams to be used.
        streams_per_process: Num of curl streams to be used per process.
        max_processes: Max processes that can be spawned.

    Returns:
        None
    """
    ctx = mp.get_context("fork")
    num_processes = math.ceil(num_streams / streams_per_process)
    num_processes = min(max(num_processes, 1), max_processes)

    logger.debug(
        "Downloading %s ranges using %s " "processes with %s streams per process",
        len(args.inputs),
        num_processes,
        streams_per_process,
    )
    t = time.perf_counter()

    # Lazy imports important for fork performance
    from multiprocessing import shared_memory  # noqa

    import certifi  # noqa
    import pycurl  # noqa

    out_queue = ctx.Queue()
    pool = []
    recieved_results = 0
    chunked_inputs = [args.inputs[i::num_processes] for i in range(num_processes)]

    # Freeze for better fork performance
    gc.freeze()

    # We are not using mp.Pool as we want to close processes as they finish.
    # Pool will keep all processes alive until until every single one finishes.
    for i, input in enumerate(chunked_inputs):
        w = ctx.Process(
            target=_worker_wrapper,
            args=(i, out_queue, callable, (input, streams_per_process, queue)),
        )
        w.name = w.name.replace("Process", "PoolWorker")
        w.daemon = True
        w.start()
        pool.append(w)

    et = time.perf_counter()
    logger.debug("Created process pool in %.2fs", et - t)

    try:
        while True:
            process_index, result = out_queue.get()
            if isinstance(result, Exception):
                raise result
            recieved_results += 1
            if recieved_results >= len(pool):
                break
    finally:
        for p in pool:
            if p.exitcode is None:
                p.terminate()
        for p in pool:
            p.join(15)

    # Put None in queue to signify that we are done
    queue.put_nowait(None)
    et = time.perf_counter()
    logger.debug("Finished downloading in %.2fs", et - t)


def capture_exception(func):
    """Decorator function that returns exceptions raised by the provided function.

    Args:
        func: The function to be decorated.

    Returns:
        The result of the decorated function or the exception raised by the function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            ret = e
        return ret

    return wrapper


def _on_curl_handle_success(
    handler: MultiCurlHandle,
    handle: "pycurl.Curl",
    *,
    queue: mp.Queue,
    start_time: float,
):
    time_s = time.perf_counter() - start_time
    output = ReadRangeOutput(
        data_start=handle.start_offset,  # type: ignore
        data_end=handle.io.offset,  # type: ignore
        logical_start=handle.logical_range[0],  # type: ignore
        logical_end=handle.logical_range[1],  # type: ignore
        url=handle.url,  # type: ignore
        time_s=time_s,
    )
    queue.put_nowait(output.to_bytes())
    del handle.io  # type: ignore


def _on_curl_handle_failure(
    handler: MultiCurlHandle,
    handle: "pycurl.Curl",
    errno: int,
    errmsg: str,
    *,
    pid: int,
    max_retries: int,
):
    if handle.retries >= max_retries:  # type: ignore
        raise RuntimeError(f"{errno} {errmsg}")
    handle.retries += 1  # type: ignore

    # We need to remove and re-add the handle to reset the failed state.
    handler.multi_handle.remove_handle(handle)
    logger.warning(
        "%s failed %s with (%s, %s), retrying (%s/%s) %s-%s...",
        pid,
        handle.url,  # type: ignore
        errno,
        errmsg,
        handle.retries,  # type: ignore
        max_retries,
        handle.range[0],  # type: ignore
        handle.range[1],  # type: ignore
    )
    handle.io.seek(handle.start_offset)  # type: ignore
    handler.multi_handle.add_handle(handle)


@capture_exception
def read_range_into_shm_sp_multi(
    args: Tuple[List[ReadRangeInput], int, mp.Queue]
) -> None:
    """
    Reads a range of data from a given URL and writes it into a shared memory buffer.

    Args:
        input: A tuple containing the following arguments:
            - fileno: The name of the shared memory buffer.
            - start_index: The starting index of the range to read.
            - end_index: The ending index of the range to read.
            - offsets: A tuple containing the buffer start index
              and data start index.
            - url: The URL from which to download the data.

    Returns:
        ReadRangeOutput: A tuple containing the following values:
            - start_offset: The actual starting offset in the shared memory buffer.
            - wrapper_offset: The current offset of the buffer file wrapper.
            - start_index: The original starting index of the range.
            - end_index: The original ending index of the range.
            - url: The URL from which the data was downloaded.
    """
    gc.disable()

    # Lazy imports important for fork performance
    from multiprocessing import shared_memory

    import certifi  # noqa
    import pycurl  # noqa

    start_time = time.perf_counter()

    pid = os.getpid()
    inputs, num_streams, queue = args
    # We assume shm_name, pycurl_settings & max_retries are the same for all inputs.
    shm_name = inputs[0].shm_name
    pycurl_settings = inputs[0].pycurl_settings or {}
    max_retries = inputs[0].max_retries or DEFAULT_MAX_RETRIES

    # Workaround for https://bugs.python.org/issue39959
    # Basically, the child processes (this) will create
    # unnecessary resource trackers that will cause issues
    # later on. We simply patch the register function out
    # to prevent that. The shm resource will still be
    # tracked by the parent process and cleaned up there.
    with patch("multiprocessing.resource_tracker.register", noop):
        shm = shared_memory.SharedMemory(name=shm_name)

    handler = MultiCurlHandle(
        on_success=partial(_on_curl_handle_success, queue=queue, start_time=start_time),
        on_failure=partial(_on_curl_handle_failure, pid=pid, max_retries=max_retries),
    )
    handler.prepare(
        num_handles=num_streams,
        inputs=inputs,
        buffer=shm.buf,
        pycurl_settings=pycurl_settings,
    )

    try:
        handler.perform()
    except Exception:
        logger.exception("Process %s failed unexpectedly", pid)
        raise
    finally:
        shm.close()
        handler.cleanup()

    et = time.perf_counter()
    time_s = et - start_time
    logger.debug("Process %s finished in %.2fs", pid, time_s)


def run_downloader(
    authkey_hex: str,
    payload_file: str,
) -> None:
    """Runs the downloader.

    The downloader will fork itself into many processes, each downloading a
    single shard of data.

    Args:
        authkey_hex: The authentication key in hexadecimal format.
        payload_file: Path to file containing the pickled payload.

    Returns:
        None

    """
    # Disable gc for better fork performance.
    # This is a very short lived process, so we don't need to worry about
    # memory leaks.
    gc.disable()
    mp.current_process().authkey = bytes.fromhex(authkey_hex)
    with open(str(payload_file), "rb") as f:
        args = pickle.load(f)
    args_bytes, queue, num_streams, streams_per_process, max_processes = args
    args = ReadRangeInputs.from_bytes(args_bytes)
    for i in args.inputs:
        i.pycurl_settings = (
            i.pycurl_settings if i.pycurl_settings is not None else args.pycurl_settings
        )
        i.max_retries = i.max_retries if i.max_retries is not None else args.max_retries
    return forked_run(
        read_range_into_shm_sp_multi,
        args,
        queue,
        num_streams,
        streams_per_process=streams_per_process,
        max_processes=max_processes,
    )


if __name__ == "__main__":
    # For forking performance, it's important that we don't import expensive and
    # unnecessary modules such as torch. See 'lazy_torch.py'.
    assert "torch" not in sys.modules, (
        "'torch' imported inside of shm_download.py. This is bad! "
        "'torch' is heavyweight and impacts fork performance. "
        "All torch imports should use 'lazy_torch.py'."
    )
    try:
        run_downloader(sys.argv[1], sys.argv[2])
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
