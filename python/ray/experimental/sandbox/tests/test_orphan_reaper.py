"""Tests for reaping the orphaned runsc helpers that a sandbox worker adopts.

Each test runs in a child Python process, since a subreaper flag and the
reaper's thread would otherwise outlive the test in the pytest process. The
helpers are copies of coreutils binaries named like runsc's, so these tests
need no gVisor.
"""

import os
import subprocess
import sys
import textwrap

import pytest

from ray.experimental.sandbox._internal import orphan_reaper

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Child subreapers are Linux-only."
)

# Shared by the child scripts: wait until a child of this process with the
# given command name is a zombie, and list this process's zombie children.
_PRELUDE = textwrap.dedent(
    """
    import ctypes, os, shutil, subprocess, sys, time
    from ray.experimental.sandbox._internal import orphan_reaper as r

    def zombies():
        found = {}
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat") as f:
                    comm, state, ppid = r._parse_stat(f.read())
            except (OSError, ValueError):
                continue
            if state == "Z" and ppid == os.getpid():
                found[int(entry)] = comm
        return found

    def wait_for_zombie(comm, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if comm in zombies().values():
                return
            time.sleep(0.02)
        raise AssertionError(f"no zombie {comm!r}: {zombies()}")

    def orphan(tmp, name, seconds):
        # Start a copy of sleep named `name` from a shell that exits at once,
        # as runsc starts runsc-fd-parking and exits before it.
        path = os.path.join(tmp, name)
        shutil.copy("/bin/sleep", path)
        out = subprocess.run(
            ["sh", "-c", f'"{path}" {seconds} & echo $!'],
            check=True, capture_output=True, text=True,
        ).stdout
        return int(out)

    def is_subreaper():
        libc = ctypes.CDLL(None, use_errno=True)
        value = ctypes.c_int(-1)
        assert libc.prctl(37, ctypes.byref(value), 0, 0, 0) == 0  # GET
        return value.value == 1

    tmp = sys.argv[1]
    """
)


def _run_child(tmp_path, body, env=None):
    script = _PRELUDE + textwrap.dedent(body) + '\nprint("OK")\n'
    child_env = dict(os.environ)
    child_env.pop(orphan_reaper.REAP_ORPHANS_ENV, None)
    child_env.update(env or {})
    proc = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=60,
        env=child_env,
    )
    assert proc.returncode == 0 and proc.stdout.strip().endswith("OK"), (
        proc.stdout + proc.stderr
    )


@pytest.fixture(autouse=True)
def _needs_coreutils():
    for binary in ("/bin/sleep", "/bin/false", "/bin/sh"):
        if not os.path.exists(binary):
            pytest.skip(f"{binary} isn't available")


def test_parse_stat_handles_parentheses_and_spaces_in_comm():
    stat = "4242 (a) (b c) Z 17 4242 4242 0 -1 4228"
    assert orphan_reaper._parse_stat(stat) == ("a) (b c", "Z", 17)


def test_reaps_orphaned_fd_parking_helper(tmp_path):
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        orphan(tmp, "runsc-fd-parking", 0.2)
        wait_for_zombie("runsc-fd-parkin")
        assert r.reap_orphans() == 1
        assert "runsc-fd-parkin" not in zombies().values()
        """,
    )


def test_reaps_orphaned_gofer(tmp_path):
    # runsc starts its gofer through /proc/self/exe, so its comm is "exe".
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        orphan(tmp, "exe", 0.2)
        wait_for_zombie("exe")
        assert r.reap_orphans() == 1
        assert "exe" not in zombies().values()
        """,
    )


def test_keeps_exit_status_of_a_popen_child_named_exe(tmp_path):
    # Programs that re-execute themselves are also named "exe"; a child this
    # process started and still waits for must keep its exit status.
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        path = os.path.join(tmp, "exe")
        shutil.copy("/bin/false", path)
        proc = subprocess.Popen([path])
        wait_for_zombie("exe")
        assert r.reap_orphans() == 0
        assert proc.wait() == 1
        """,
    )


def test_leaves_other_orphans_alone(tmp_path):
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        orphan(tmp, "other-helper", 0.1)
        wait_for_zombie("other-helper")
        assert r.reap_orphans() == 0
        assert list(zombies().values()) == ["other-helper"]
        for pid in zombies():
            os.waitpid(pid, 0)
        """,
    )


def test_keeps_runsc_exit_status_for_popen(tmp_path):
    # The backend runs runsc itself through subprocess, which needs to reap
    # it to read its exit status.
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        path = os.path.join(tmp, "runsc")
        shutil.copy("/bin/false", path)
        proc = subprocess.Popen([path])
        wait_for_zombie("runsc")
        assert r.reap_orphans() == 0
        assert proc.wait() == 1
        """,
    )


def test_start_runs_reaper_thread_once(tmp_path):
    _run_child(
        tmp_path,
        """
        import threading
        assert not is_subreaper()
        assert r.start()
        assert r.start()
        assert is_subreaper()
        names = [t.name for t in threading.enumerate()]
        assert names.count("ray-sandbox-orphan-reaper") == 1, names
        pid = orphan(tmp, "runsc-fd-parking", 0.1)
        # The helper is gone from /proc only once it has been reaped.
        deadline = time.monotonic() + 10
        while os.path.exists(f"/proc/{pid}"):
            assert time.monotonic() < deadline, zombies()
            time.sleep(0.05)
        """,
        env={orphan_reaper.REAP_INTERVAL_ENV: "0.05"},
    )


@pytest.mark.parametrize("value", ["0", "false", "off"])
def test_env_turns_reaper_off(tmp_path, value):
    _run_child(
        tmp_path,
        """
        assert not r.start()
        assert not is_subreaper()
        """,
        env={orphan_reaper.REAP_ORPHANS_ENV: value},
    )


def test_forked_child_starts_its_own_reaper(tmp_path):
    _run_child(
        tmp_path,
        """
        assert r.start()
        pid = os.fork()
        if pid == 0:
            ok = r._thread is None and not is_subreaper() and r.start()
            ok = ok and is_subreaper()
            os._exit(0 if ok else 1)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        """,
    )


def test_does_nothing_off_linux(monkeypatch):
    monkeypatch.setattr(orphan_reaper.sys, "platform", "darwin")
    monkeypatch.setattr(orphan_reaper, "_thread", None)
    monkeypatch.setattr(
        orphan_reaper,
        "_enable_subreaper",
        lambda: pytest.fail("prctl called off Linux"),
    )
    assert not orphan_reaper.start()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
