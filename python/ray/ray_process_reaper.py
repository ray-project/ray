import atexit
import os
import signal
import sys
import time
"""
This is a lightweight "reaper" process used to ensure that ray processes are
cleaned up properly when the main ray process dies unexpectedly (e.g.,
segfaults or gets SIGKILLed). Note that processes may not be cleaned up
properly if this process is SIGTERMed or SIGKILLed.

It detects that its parent has died by reading from stdin, which must be
inherited from the parent process so that the OS will deliver an EOF if the
parent dies. When this happens, the reaper process kills the rest of its
process group (first attempting graceful shutdown with SIGTERM, then escalating
to SIGKILL).
"""

SIGTERM_GRACE_PERIOD_SECONDS = 1


def reap_process_group(*args):
    def sigterm_handler(*args):
        # Give a one-second grace period for other processes to clean up.
        time.sleep(SIGTERM_GRACE_PERIOD_SECONDS)
        # SIGKILL the pgroup (including ourselves) as a last-resort.
        if sys.platform == "win32":
            atexit.unregister(sigterm_handler)
            os.kill(0, signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(0, signal.SIGKILL)

    # Set a SIGTERM handler to handle SIGTERMing ourselves with the group.
    if sys.platform == "win32":
        atexit.register(sigterm_handler)
    else:
        signal.signal(signal.SIGTERM, sigterm_handler)

    # Our parent must have died, SIGTERM the group (including ourselves).
    if sys.platform == "win32":
        os.kill(0, signal.CTRL_C_EVENT)
    else:
        os.killpg(0, signal.SIGTERM)


def main():
    # Read from stdout forever. Because stdout is a file descriptor
    # inherited from our parent process, we will get an EOF if the parent
    # dies, which is signaled by an empty return from read().
    # We intentionally don't set any signal handlers here, so a SIGTERM from
    # the parent can be used to kill this process gracefully without it killing
    # the rest of the process group.
    while len(sys.stdin.read()) != 0:
        pass
    reap_process_group()


if __name__ == "__main__":
    main()
