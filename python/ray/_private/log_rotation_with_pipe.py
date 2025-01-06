"""Implement log rotation with pipe."""

from typing import IO, AnyStr
import sys
import os
import logging
import threading
import uuid
import atexit
from logging.handlers import RotatingFileHandler

# Use a special token to indicate write finish.
_EOF_TOKEN = str(uuid.uuid4()) + "\n"


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


def open_pipe_with_file_handlers(log_name: str, handlers) -> IO[AnyStr]:
    """Stream content into pipe, which will be listened and dumped to provided
    handlers."""
    read_fd, write_fd = os.pipe()

    log_content = []
    stopped = threading.Event()
    cond = threading.Condition()

    # Have to use different logger names for different sinks.
    logger = logging.getLogger(f"logger-{log_name}")
    logger.setLevel(logging.INFO)
    for handler in handlers:
        logger.addHandler(handler)

    # Setup read thread, which continuous read content out of pipe and append to buffer.
    def read_log_content_from_pipe():
        with os.fdopen(read_fd, "r") as pipe_reader:
            while True:
                line = pipe_reader.readline()

                with cond:
                    # An empty line is read over, which indicates write side has
                    # closed.
                    if line == _EOF_TOKEN:
                        stopped.set()
                        cond.notify()
                        return

                    # Only buffer new line when not empty.
                    stripped = line.strip()
                    if not stripped:
                        continue

                    # Append content and continue.
                    log_content.append(stripped)
                    cond.notify()

    # Setup logging thread, which continues read content out of log buffer and persist
    # via logger.
    # Two threads are used here to make sure continuous read from pipe thus avoid
    # blocking write from application.
    def dump_log_content_to_buffer():
        # Current line to log.
        cur_content = None

        while True:
            with cond:
                while not log_content and not stopped.is_set():
                    cond.wait()

                if log_content:
                    cur_content = log_content.pop(0)
                else:
                    assert stopped.is_set()
                    return

            # Log and perform IO operation out of critical section.
            logger.info(cur_content)

    # Python `atexit` hook only invokes when no other non-daemon threads running.
    read_thread = threading.Thread(target=read_log_content_from_pipe, daemon=True)
    dump_thread = threading.Thread(target=dump_log_content_to_buffer, daemon=True)
    read_thread.start()
    dump_thread.start()

    pipe_write_stream = PipeStreamWriteHandle(write_fd, log_name=log_name)

    def cleanup():
        pipe_write_stream.write(_EOF_TOKEN)
        pipe_write_stream.flush()
        pipe_write_stream.close()

        # Synchronize to make sure everything flushed.
        read_thread.join()
        dump_thread.join()

    atexit.register(cleanup)

    return pipe_write_stream


def open_pipe_with_rotation(
    fname: str, rotation_max_size: int = sys.maxsize, rotation_file_num: int = 1
) -> IO[AnyStr]:
    """Stream content into pipe, which will be listened and dumped to [fname] with
    rotation."""
    handler = RotatingFileHandler(
        fname, maxBytes=rotation_max_size, backupCount=rotation_file_num
    )
    # Only logging message with nothing else.
    handler.setFormatter(logging.Formatter("%(message)s"))
    return open_pipe_with_file_handlers(fname, [handler])
