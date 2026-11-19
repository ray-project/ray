"""Linux Jobs entrypoint process and its process-group watchdog.

The watchdog runs this file in an isolated interpreter using only the standard
library. It leads the driver's process group and waits for Core Worker to own
that group before launching the entrypoint.
"""

import json
import logging
import os
import select
import signal
import subprocess
import sys
from typing import Callable, Dict, Optional, TextIO

logger = logging.getLogger(__name__)

_START_DRIVER = b"1"
_REAP_DRIVER = b"2"
_DRIVER_REAP_TIMEOUT_S = 1


class JobProcess:
    """Keep the group's leader waitable until native cleanup retires its PGID."""

    def __init__(
        self,
        register_group: Callable[[int], None],
        cleanup_group: Callable[[], None],
    ):
        self.returncode: Optional[int] = None
        self._register_group = register_group
        self._cleanup_group = cleanup_group
        self._registered = False
        self._helper = None
        self._control_write: Optional[int] = None

    def start(self, entrypoint: str, env: Dict[str, str], logs_file: TextIO):
        control_read, self._control_write = os.pipe()
        try:
            self._helper = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    os.path.abspath(__file__),
                    str(control_read),
                    str(logs_file.fileno()),
                    entrypoint,
                ],
                start_new_session=True,
                env=env,
                pass_fds=(control_read, logs_file.fileno()),
                stdout=subprocess.PIPE,
                stderr=logs_file,
                bufsize=0,
            )
            # Popen has completed setsid/exec. The helper waits on the control
            # pipe, so registration failure cannot leave a running entrypoint.
            self._register_group(self._helper.pid)
            self._registered = True
            os.write(self._control_write, _START_DRIVER)
            message = self._read_message(timeout=30)
            if message is None or "pid" not in message:
                raise RuntimeError("Timed out starting the Jobs entrypoint")
            self.pid = message["pid"]
        except BaseException:
            self._close_after_error()
            raise
        finally:
            os.close(control_read)

    def _read_message(self, timeout: float):
        poller = select.poll()
        poller.register(self._helper.stdout, select.POLLIN)
        if not poller.poll(timeout * 1000):
            return None
        line = self._helper.stdout.readline()
        if not line:
            raise RuntimeError("Jobs process watchdog exited before reporting status")
        message = json.loads(line)
        if "error" in message:
            raise RuntimeError(message["error"])
        return message

    def poll(self) -> Optional[int]:
        """Read the driver's status while reserving its PID until explicit cleanup."""
        if self._control_write is None or self.returncode is not None:
            return self.returncode
        try:
            message = self._read_message(timeout=0)
            if message is not None:
                self.returncode = message["returncode"]
        except BaseException:
            self._close_after_error()
            raise
        return self.returncode

    def _close_after_error(self):
        try:
            self.close()
        except Exception:
            logger.exception("Failed to clean up the Jobs watchdog")

    def _reap_driver(self):
        # A graceful stop can reach here before the watchdog has reported the
        # driver's exit. The exit message means its PID is still reserved.
        try:
            if self.returncode is None:
                message = self._read_message(timeout=_DRIVER_REAP_TIMEOUT_S)
                if message is None:
                    raise RuntimeError("Timed out waiting for driver exit status")
                self.returncode = message["returncode"]
            os.write(self._control_write, _REAP_DRIVER)
            message = self._read_message(timeout=_DRIVER_REAP_TIMEOUT_S)
            if message != {"driver_reaped": True}:
                raise RuntimeError("Watchdog did not confirm driver reaping")
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            logger.warning(
                "Could not reap the Jobs driver before group cleanup: %s", error
            )

    def close(self, *, reap_driver: bool = False):
        """Clean the process group and release the helper's PID.

        Args:
            reap_driver: Ask the watchdog to reap the driver before group
                cleanup. The caller must be done using the driver's PID.
        """
        if self._control_write is not None:
            if self._helper is not None:
                if self._registered:
                    if reap_driver:
                        self._reap_driver()
                    self._cleanup_group()
                else:
                    try:
                        os.killpg(self._helper.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                self._helper.stdout.close()
            os.close(self._control_write)
            self._control_write = None

        if self._helper is not None:
            # Keep the helper waitable until native cleanup retires the PGID.
            # Once the pipes are closed, retries only need to reap the helper.
            try:
                self._helper.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning(
                    "Timed out reaping the Jobs watchdog %s", self._helper.pid
                )
            else:
                self._helper = None


def _report(message):
    os.write(sys.stdout.fileno(), json.dumps(message).encode() + b"\n")


def _run_watchdog(control_fd: int, logs_fd: int, entrypoint: str):
    driver = None
    try:
        # Group-wide stop signals must leave the watchdog alive to report the
        # driver's exit. Caught handlers reset to SIG_DFL when Popen execs the
        # shell, so the driver retains its normal signal behavior.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: None)
        if os.read(control_fd, 1) != _START_DRIVER:
            return
        # The isolated helper has no Python threads. Unblock SIGINT here before
        # starting a driver that requests SIGINT for normal job stop.
        if os.environ.get("RAY_JOB_STOP_SIGNAL") == "SIGINT":
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        driver = subprocess.Popen(
            entrypoint,
            shell=True,
            stdout=logs_fd,
            stderr=subprocess.STDOUT,
        )
        os.close(logs_fd)
        _report({"pid": driver.pid})
        poller = select.poll()
        poller.register(control_fd, select.POLLIN)
        while not poller.poll(100):
            # The supervisor may still use the driver PID for a concurrent stop.
            info = os.waitid(os.P_PID, driver.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if info is not None:
                returncode = (
                    info.si_status if info.si_code == os.CLD_EXITED else -info.si_status
                )
                _report({"returncode": returncode})
                request = os.read(control_fd, 1)
                # Both a reap request and EOF release the driver's PID. The
                # helper itself stays waitable until the group is cleaned.
                driver.wait()
                if request == _REAP_DRIVER:
                    _report({"driver_reaped": True})
                    os.read(control_fd, 1)
                return
        if not os.read(control_fd, 1):
            driver.poll()
    except BrokenPipeError:
        # The supervisor closed the status pipe during shutdown.
        if driver is not None:
            driver.poll()
    except Exception as error:
        _report({"error": str(error)[:1024]})
    finally:
        # The helper is the group leader and handles abrupt supervisor loss.
        os.killpg(os.getpgrp(), signal.SIGKILL)


if __name__ == "__main__":
    _run_watchdog(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3])
