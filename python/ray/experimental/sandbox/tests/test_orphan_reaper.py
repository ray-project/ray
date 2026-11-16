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
                    comm, state, ppid, _ = r._parse_stat(f.read())
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

    def orphan(tmp, name, seconds, own_session=True):
        # Start a copy of sleep named `name` from a shell that exits at once,
        # as runsc starts runsc-fd-parking and exits before it. runsc starts
        # its helpers in sessions of their own (setsid).
        path = os.path.join(tmp, name)
        shutil.copy("/bin/sleep", path)
        setsid = "setsid " if own_session else ""
        out = subprocess.run(
            ["sh", "-c", f'{setsid}"{path}" {seconds} & echo $!'],
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
    stat = "4242 (a) (b c) Z 17 4243 4244 0 -1 4228"
    assert orphan_reaper._parse_stat(stat) == ("a) (b c", "Z", 17, 4244)


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


def test_leaves_an_orphan_in_this_session_alone(tmp_path):
    # Only helpers that runsc started in their own session are reaped.
    _run_child(
        tmp_path,
        """
        assert r._enable_subreaper()
        orphan(tmp, "exe", 0.1, own_session=False)
        wait_for_zombie("exe")
        assert r.reap_orphans() == 0
        for pid in zombies():
            os.waitpid(pid, 0)
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


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1", "soon"])
def test_interval_falls_back_to_default_unless_positive_and_finite(monkeypatch, value):
    monkeypatch.setenv(orphan_reaper.REAP_INTERVAL_ENV, value)
    assert orphan_reaper._interval_seconds() == orphan_reaper._DEFAULT_INTERVAL_SECONDS


def test_interval_from_env(monkeypatch):
    monkeypatch.setenv(orphan_reaper.REAP_INTERVAL_ENV, "0.5")
    assert orphan_reaper._interval_seconds() == 0.5


def test_enable_subreaper_without_libc_returns_false(monkeypatch):
    def no_libc(*args, **kwargs):
        raise OSError("no libc")

    monkeypatch.setattr(orphan_reaper.ctypes, "CDLL", no_libc)
    assert not orphan_reaper._enable_subreaper()


def test_enable_subreaper_without_prctl_returns_false(monkeypatch):
    monkeypatch.setattr(orphan_reaper.ctypes, "CDLL", lambda *a, **k: object())
    assert not orphan_reaper._enable_subreaper()


def test_reap_now_does_nothing_without_the_reaper(monkeypatch):
    monkeypatch.setattr(orphan_reaper, "_thread", None)
    monkeypatch.setattr(
        orphan_reaper, "reap_orphans", lambda: pytest.fail("scanned /proc")
    )
    assert orphan_reaper.reap_now() == 0


def test_reap_now_reaps_before_the_next_pass(tmp_path):
    # With an interval far longer than the test, only reap_now can reap the
    # helper, as when the worker exits right after deleting a sandbox.
    _run_child(
        tmp_path,
        """
        assert r.start()
        orphan(tmp, "runsc-fd-parking", 0.1)
        wait_for_zombie("runsc-fd-parkin")
        assert r.reap_now() == 1
        assert "runsc-fd-parkin" not in zombies().values()
        """,
        env={orphan_reaper.REAP_INTERVAL_ENV: "600"},
    )


def test_delete_sandbox_reaps_helpers_before_returning(monkeypatch):
    from ray.experimental.sandbox.backend import gvisor

    calls = []
    backend = gvisor.GVisorSandboxBackend.__new__(gvisor.GVisorSandboxBackend)
    backend._sandbox_metadata = {
        "sb": {"root_dir": "/nonexistent", "config": None, "cgroup": None}
    }
    backend._runsc_base_args = lambda config: ["runsc"]
    backend._delete_container_state = lambda config, sandbox_id: None
    monkeypatch.setattr(gvisor.subprocess, "run", lambda *a, **k: None)
    monkeypatch.setattr(gvisor, "_release_cgroup", lambda *a: False)
    monkeypatch.setattr(orphan_reaper, "reap_now", lambda: calls.append("reap"))
    backend.delete_sandbox("sb")
    assert calls == ["reap"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
