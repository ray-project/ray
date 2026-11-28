"""Exec a command that the kernel kills when the process that spawned it dies.

Usage: python -I -S fate_share_exec.py <parent pid> <command> [args...]
"""
import ctypes
import os
import signal
import sys

PR_SET_PDEATHSIG = 1

if __name__ == "__main__":
    prctl = ctypes.CDLL(None, use_errno=True).prctl
    prctl.restype = ctypes.c_int
    prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    if prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    # A parent that died before the prctl took effect will never send it.
    if os.getppid() != int(sys.argv[1]):
        sys.exit(1)
    os.execvp(sys.argv[2], sys.argv[2:])
