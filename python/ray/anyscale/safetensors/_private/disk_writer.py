import concurrent.futures
import os
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from queue import Queue
from typing import IO, Dict, List, Tuple, Union
from uuid import uuid4

from ray.anyscale.safetensors._private.base_sink import BaseSink
from ray.anyscale.safetensors._private.env import PROCESS_SHUTDOWN_TIMEOUT_S
from ray.anyscale.safetensors._private.logging_utils import logger
from ray.anyscale.safetensors._private.shm_download import ReadRangeOutput
from ray.anyscale.safetensors._private.util import SafetensorsMetadata

TMP_SUFFIX = ".tmp"


def url_or_path_to_base_name(url_or_path: str) -> str:
    return os.path.basename(url_or_path)


def base_name_get_path_with_suffix(path: Path, suffix: str) -> Path:
    return path.with_suffix(path.suffix + suffix)


@dataclass
class FileWriteMetadata:
    raw_metadata: bytes
    buffer_range: Tuple[int, int]

    @property
    def metadata_size_bytes(self) -> int:
        return len(self.raw_metadata)

    @property
    def total_size_bytes(self) -> int:
        return self.metadata_size_bytes + (self.buffer_range[1] - self.buffer_range[0])

    def buffer_range_to_file_range(self, range: Tuple[int, int]) -> Tuple[int, int]:
        assert range[0] >= self.buffer_range[0] and range[1] <= self.buffer_range[1]
        return (
            range[0] - self.buffer_range[0] + self.metadata_size_bytes,
            range[1] - self.buffer_range[0] + self.metadata_size_bytes,
        )


@dataclass(frozen=True)
class FileInfo:
    base_name: str
    full_path: Path
    temp_path: Path
    temp_file: IO
    metadata: FileWriteMetadata

    @classmethod
    def from_base_paths_and_metadata(
        cls, base_path: Path, base_name: str, metadata: FileWriteMetadata, id: str
    ):
        path = base_path / base_name
        tmp_path = base_name_get_path_with_suffix(path, f".{id}" + TMP_SUFFIX)
        return FileInfo(base_name, path, tmp_path, open(tmp_path, "w+b"), metadata)

    def close(self):
        try:
            self.temp_file.close()
        except Exception:
            pass

    def __del__(self):
        self.close()


class WriterSentinelValue(Enum):
    FINISH_SUCCESS = 0
    FINISH_ERROR = 1


def _write_buffer_range_to_file(
    buffer: memoryview,
    fp: IO,
    range_in_buffer: Tuple[int, int],
    file_write_metadata: FileWriteMetadata,
):
    """Write range_in_buffer slice of data from buffer to fp.

    file_write_metadata is used to determine the correct offset.
    """
    range_in_file = file_write_metadata.buffer_range_to_file_range(range_in_buffer)
    fp.seek(range_in_file[0])
    fp.write(buffer[range_in_buffer[0] : range_in_buffer[1]])


def writer_thread(
    base_path: Path,
    base_names_to_file_write_metadata: Dict[str, FileWriteMetadata],
    buffer: memoryview,
    queue: Queue,
):
    """
    Writes data from a buffer to files on disk.

    This function will open temporary files (with unique suffix) in base_path and write
    to them by using the slice info coming from the queue. It will atomically rename
    the temporary files to final files if it recieves a FINISH_SUCCESS sentinel value
    from the queue, or remove the temporary files if it recieves a FINISH_ERROR sentinel
    value.

    Args:
        base_path: The base path where the files will be written.
        base_names_to_file_write_metadata: A dictionary mapping base names to file
            write metadata, which controls which slice of the buffer corresponds
            to which file.
        buffer: The buffer containing the data to be written.
        queue: The queue used for communication with the writer thread.

    Raises:
        Exception: If an error occurs during the writing process.

    Returns:
        None
    """
    os.makedirs(base_path, exist_ok=True)
    guid = uuid4().hex
    success = False
    base_names_to_fileinfo = {}

    try:
        base_names_to_fileinfo = {
            base_name: FileInfo.from_base_paths_and_metadata(
                base_path, base_name, metadata, guid
            )
            for base_name, metadata in base_names_to_file_write_metadata.items()
        }

        # prepare by writing raw metadata
        for base_name, metadata in base_names_to_file_write_metadata.items():
            fp = base_names_to_fileinfo[base_name].temp_file
            fp.write(metadata.raw_metadata)

        while True:
            item: Union[ReadRangeOutput, WriterSentinelValue] = queue.get()
            if isinstance(item, WriterSentinelValue):
                queue.task_done()
                success = item == WriterSentinelValue.FINISH_SUCCESS
                break
            base_name = url_or_path_to_base_name(item.url)
            file_write_metadata = base_names_to_file_write_metadata[base_name]
            fp = base_names_to_fileinfo[base_name].temp_file
            _write_buffer_range_to_file(
                buffer, fp, (item.data_start, item.data_end), file_write_metadata
            )
            queue.task_done()
    except Exception:
        logger.exception("Disk writer failed with unexpected error")
        raise
    finally:
        for fileinfo in base_names_to_fileinfo.values():
            fileinfo.close()

    if success:
        for fileinfo in base_names_to_fileinfo.values():
            fileinfo.temp_path.replace(fileinfo.full_path)
        logger.info("Finished writing to disk")
    else:
        for fileinfo in base_names_to_fileinfo.values():
            fileinfo.temp_path.unlink(missing_ok=True)
        logger.info("Aborted writing to files")


class DiskWriter(BaseSink):
    """
    A class for writing data to disk asynchronously.

    Args:
        base_path: The base path where the files will be written.
        buffer: The buffer containing the data to be written.
        metadatas: A list of metadata objects containing information about the data.
        start_on_enter: Whether to start the writer thread automatically when entering
            a context.
    """

    def __init__(
        self,
        base_path: Path,
        buffer: memoryview,
        metadatas: List[SafetensorsMetadata],
        start_on_enter: bool = True,
        async_flush: bool = True,
    ) -> None:
        self.async_flush = async_flush
        self.start_on_enter = start_on_enter
        self.base_names_to_file_write_metadata = {}
        self.buffer = buffer
        self.base_path = Path(base_path)
        self.queue: Queue[Union[ReadRangeOutput, WriterSentinelValue]] = Queue()
        self.thread = None
        offset = 0
        for metadata in metadatas:
            base_name = url_or_path_to_base_name(metadata.path_or_url)
            self.base_names_to_file_write_metadata[base_name] = FileWriteMetadata(
                metadata.raw_metadata, (offset, offset + metadata.data_size_bytes)
            )
            offset += metadata.data_size_bytes

    def __enter__(self):
        if self.start_on_enter:
            self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown(success=exc_type is None)

    def start(self):
        """
        Start the writer thread.
        """
        logger.debug("Starting disk writer thread, base_path=%s", self.base_path)
        self.thread = threading.Thread(
            target=writer_thread,
            kwargs=dict(
                base_path=self.base_path,
                base_names_to_file_write_metadata=self.base_names_to_file_write_metadata,  # noqa: E501
                buffer=self.buffer,
                queue=self.queue,
            ),
            daemon=True,
        )
        self.thread.start()

    def process_output(self, item: ReadRangeOutput):
        """
        Process the output from downloader.

        Args:
            item: The output item to be processed.
        """
        self._put_in_queue(item)

    def _put_in_queue(self, item: Union[ReadRangeOutput, WriterSentinelValue]):
        if self.thread and self.thread.is_alive():
            self.queue.put_nowait(item)
        else:
            raise RuntimeError("Disk writer thread is not instantiated or dead.")

    def _shutdown(self, success: bool, timeout_s: float):
        if self.thread and self.thread.is_alive():
            self._put_in_queue(
                WriterSentinelValue.FINISH_SUCCESS
                if success
                else WriterSentinelValue.FINISH_ERROR
            )
            self.queue.join()
            self.thread.join(timeout=timeout_s)

        if self.thread and self.thread.is_alive():
            raise TimeoutError(
                f"Timed out after {timeout_s}s waiting "
                "for Disk writer thread to exit."
            )

    def shutdown(
        self,
        *,
        timeout_s: float = PROCESS_SHUTDOWN_TIMEOUT_S,
        success: bool = True,
    ):
        """
        Shutdown the writer thread.

        This can be called multiple times. It will block until the thread is finished.

        Args:
            success: Whether the shutdown occured due to successful
                completion of the download or not.
        """
        future: concurrent.futures.Future = concurrent.futures.Future()

        def _async_shutdown():
            try:
                future.set_result(self._shutdown(success, timeout_s))
            except Exception as e:
                future.set_exception(e)

        if self.async_flush:
            threading.Thread(target=_async_shutdown).start()
            return future
        else:
            self._shutdown(success, timeout_s)

    def __del__(self):
        self.shutdown(success=True)


class DummyDiskWriter(BaseSink):
    """
    Implementation of BaseDiskWriter that does nothing.

    Used if disk writing is disabled.
    """

    def start(self):
        return

    def process_output(self, item: ReadRangeOutput):
        return

    def shutdown(self, *, timeout_s: float, success: bool, async_flush: bool = False):
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown(success=exc_type is None)
