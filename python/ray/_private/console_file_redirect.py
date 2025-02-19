import atexit
import logging
import os
from pathlib import Path
import sys
import threading
import time

from ray._private.utils import sync_file_tail_iterator

logger = logging.getLogger(__name__)


def setup_console_redirection_to_file(filepath: Path):
    """
    Redirects the console output to a file and tail the log to output to console.
    """
    logger.info(f"Redirecting console output to {filepath}...")
    log_file = open(filepath, "a")

    # Keep the original stdout file descriptor so that the console log in the file
    # can be output to back to console.
    original_stdout_fileno = os.dup(sys.stdout.fileno())
    os.dup2(log_file.fileno(), sys.stderr.fileno())
    os.dup2(log_file.fileno(), sys.stdout.fileno())

    # tail the log and output to console
    tail_console_log_thread = ConsoleLogTailer(filepath, original_stdout_fileno)
    tail_console_log_thread.start()
    while not tail_console_log_thread.is_started():
        time.sleep(0.1)

    # cleanup during program exit
    def on_exit():
        # stop the file tailing thread
        tail_console_log_thread.shutdown()
        tail_console_log_thread.join()

        # close the log file
        log_file.close()

    atexit.register(on_exit)


class ConsoleLogTailer(threading.Thread):
    """
    A thread that tails a log file and outputs the log to console.
    """

    def __init__(self, filepath: Path, original_stdout_fileno: int):
        super().__init__(daemon=True)
        self.filepath = filepath
        self.original_stdout_fileno = original_stdout_fileno
        self._shutdown_event = threading.Event()
        self._start_event = threading.Event()

    def run(self):
        logger.info(f"Tailing log file from {self.filepath}...")

        for lines in sync_file_tail_iterator(str(self.filepath)):
            self._start_event.set()
            if lines is not None:
                for line in lines:
                    os.write(self.original_stdout_fileno, line.encode())

            elif self._shutdown_event.is_set():
                break

    def is_started(self):
        return self._start_event.is_set()

    def shutdown(self):
        self._shutdown_event.set()
