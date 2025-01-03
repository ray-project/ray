"""Implement log rotation with pipe."""

from typing import IO, AnyStr
import sys
import os
import logging
import threading
import atexit
from logging.handlers import RotatingFileHandler


class PipeStreamWriteHandle(IO[AnyStr]):
    def __init__(self, write_fd: int, log_name: str):
        self.name = log_name
        self.write_fd = write_fd
        self.stream = os.fdopen(write_fd, "w")

    def name(self):
        return self.name

    def fileno(self):
        return self.write_fd

    def write(self, s: str) -> int:
        return self.stream.write(s)

    def flush(self):
        self.stream.flush()

    def close(self):
        self.stream.close()

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


def open_pipe_with_rotation(
    fname: str, rotation_max_size: int = sys.maxsize, rotation_file_num: int = 1
) -> IO[AnyStr]:
    """Stream content into pipe, which will be listened and dumped to [fname] with
    rotation."""
    read_fd, write_fd = os.pipe()

    log_content = []
    stopped = False
    lock = threading.Lock()
    cond = threading.Condition(lock)

    logger = logging.getLogger(name=fname)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        fname, maxBytes=rotation_max_size, backupCount=rotation_file_num
    )
    # Only logging message with nothing else.
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    # Setup read thread, which continuous read content out of pipe and append to buffer.
    def read_log_content_from_pipe():
        with os.fdopen(read_fd, "r") as pipe_reader:
            while True:
                line = pipe_reader.readline()

                # An empty line is read over, which indicates write side has closed.
                if line == "":
                    with cond:
                        cond.notify()
                    return

                # Only buffer new line when not empty.
                line = line.strip()
                if line:
                    with cond:
                        log_content.append(line)
                        cond.notify()

    # Setup logging thread, which continues read content out of log buffer and persist
    # via logger.
    # Two threads are used here to make sure continuous read from pipe thus avoid
    # blocking write from application.
    def dump_log_content_to_buffer():
        while True:
            with cond:
                while not log_content and not stopped:
                    cond.wait()

                if log_content:
                    content = log_content.pop(0)
                    logger.info(content)
                    continue

                # Thread requested to stop
                return

    read_thread = threading.Thread(target=read_log_content_from_pipe)
    dump_thread = threading.Thread(target=dump_log_content_to_buffer)
    read_thread.start()
    dump_thread.start()

    pipe_write_stream = PipeStreamWriteHandle(write_fd, fname)

    def cleanup():
        nonlocal stopped
        with lock:
            stopped = True
        pipe_write_stream.close()

    atexit.register(cleanup)

    return pipe_write_stream
