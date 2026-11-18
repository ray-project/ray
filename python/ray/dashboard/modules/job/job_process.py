"""Linux Jobs entrypoint process and its process-group watchdog.

The watchdog runs this file in an isolated interpreter using only the standard
library. It leads the driver's process group and waits for Core Worker to own
that group before launching the entrypoint.
"""

import json
import os
import select
import signal
import subprocess
import sys
from typing import Callable, Dict, Optional, TextIO


class JobProcess:
    """Keep the group's leader waitable until native cleanup retires its PGID."""

    def __init__(
        self,
        entrypoint: str,
        env: Dict[str, str],
        logs_file: TextIO,
        register_group: Callable[[int], None],
        cleanup_group: Callable[[], None],
    ):
        self.returncode: Optional[int] = None
        self._cleanup_group = cleanup_group
        self._registered = False
        self._closed = False
        self._helper = None
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
            register_group(self._helper.pid)
            self._registered = True
            os.write(self._control_write, b"1")
            message = self._read_message(timeout=30)
            if message is None or "pid" not in message:
                raise RuntimeError("Timed out starting the Jobs entrypoint")
            self.pid = message["pid"]
        except BaseException:
            self.close()
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
        """Read the driver's status, then clean its group before reaping the helper."""
        if self._closed:
            return self.returncode
        try:
            message = self._read_message(timeout=0)
            if message is not None:
                self.returncode = message["returncode"]
                self.close()
        except BaseException:
            self.close()
            raise
        return self.returncode

    def close(self):
        """Release the process identity only after group cleanup succeeds."""
        if self._closed:
            return
        if self._helper is not None:
            # Never call Popen.poll()/wait() before cleanup. Even a dead helper
            # reserves its PID while it remains an unreaped child.
            if self._registered:
                self._cleanup_group()
            else:
                try:
                    os.killpg(self._helper.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            self._helper.wait(timeout=5)
            self._helper.stdout.close()
        os.close(self._control_write)
        self._closed = True


def _report(message):
    os.write(sys.stdout.fileno(), json.dumps(message).encode() + b"\n")


def _run_watchdog(control_fd: int, logs_fd: int, entrypoint: str):
    try:
        # Group-wide stop signals must leave the watchdog alive to report the
        # driver's exit. Caught handlers reset to SIG_DFL when Popen execs the
        # shell, so the driver retains its normal signal behavior.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: None)
        if os.read(control_fd, 1) != b"1":
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
        reported_exit = False
        poller = select.poll()
        poller.register(control_fd, select.POLLIN)
        while True:
            if poller.poll(100) and not os.read(control_fd, 1):
                return
            if not reported_exit:
                # Reserve the driver's PID until group cleanup. The supervisor
                # may still be using it to handle a concurrent stop request.
                info = os.waitid(
                    os.P_PID, driver.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT
                )
                if info is not None:
                    returncode = (
                        info.si_status
                        if info.si_code == os.CLD_EXITED
                        else -info.si_status
                    )
                    _report({"returncode": returncode})
                    reported_exit = True
    except Exception as error:
        _report({"error": str(error)[:1024]})
    finally:
        # The live helper is the group leader. This also handles supervisor
        # SIGKILL, which closes the pipe without running native cleanup.
        os.killpg(os.getpgrp(), signal.SIGKILL)


if __name__ == "__main__":
    _run_watchdog(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3])
