"""The sandbox supervisor and the teardown it runs (``_internal/teardown``).

Most tests drive a fake ``runsc`` that records its calls, real process groups,
and a stand-in mount-s3, so they need no gVisor; the last ones SIGKILL a process
that owns a real sandbox. Needs TEST_SANDBOX=1.
"""

import fcntl
import glob
import json
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from ray.experimental.sandbox._internal import teardown

SANDBOX_ID = "ray-sandbox-0123456789ab"
OTHER_ID = "ray-sandbox-ba9876543210"

# Records its arguments. "delete" ends the container, which a stand-in
# mount-s3 serving it notices (see _mount_dir).
_FAKE_RUNSC = r"""#!/bin/sh
dir=$(dirname "$0")
echo "$@" >> "$dir/runsc.calls"
for a; do [ "$a" = delete ] && touch "$dir/container-gone"; done
exit 0
"""


class _Host:
    """A node in tmp: the sandbox, mount and image directories, and runsc."""

    def __init__(self, tmp_path):
        self.bin = tmp_path / "bin"
        self.sandboxes = tmp_path / "sandbox"
        self.mounts = tmp_path / "mounts"
        self.images = tmp_path / "images"
        self.runsc_root = str(tmp_path / "runsc")
        for path in (self.bin, self.sandboxes, self.mounts, self.images):
            path.mkdir()
        runsc = self.bin / "runsc"
        runsc.write_text(_FAKE_RUNSC)
        runsc.chmod(0o755)

    @property
    def runsc_args(self):
        return ["runsc", "--root", self.runsc_root]

    def calls(self):
        path = self.bin / "runsc.calls"
        return path.read_text().splitlines() if path.exists() else []

    def make_sandbox(self, sandbox_id=SANDBOX_ID, **owner):
        """A sandbox directory with an owner.json (``owner`` overrides fields
        of this process's) and an image pin."""
        root = self.sandboxes / sandbox_id
        (root / "rootfs").mkdir(parents=True)
        (root / "config.json").write_text("{}")
        teardown.write_owner(str(root), rootless=False, ignore_cgroups=False)
        if owner:
            record = json.loads((root / "owner.json").read_text())
            record.update(owner)
            (root / "owner.json").write_text(json.dumps(record))
        self.pin(sandbox_id)
        return root

    def pin(self, sandbox_id):
        pin = self.images / "busybox_latest" / ".users" / sandbox_id
        pin.parent.mkdir(parents=True, exist_ok=True)
        pin.touch()
        return pin

    def teardown(self, sandbox_id=SANDBOX_ID, grace_s=5.0):
        return teardown.teardown_sandbox(
            sandbox_id,
            self.runsc_args,
            str(self.sandboxes),
            str(self.mounts),
            str(self.images),
            grace_s=grace_s,
        )

    def sweep(self):
        return teardown.sweep_orphaned_sandboxes(
            str(self.sandboxes), str(self.mounts), str(self.images), self.runsc_root
        )

    def supervisor(self, sandbox_id=SANDBOX_ID):
        return teardown.SandboxSupervisor.start(
            sandbox_id,
            self.runsc_args,
            str(self.sandboxes),
            str(self.mounts),
            str(self.images),
        )


@pytest.fixture
def host(tmp_path, monkeypatch):
    node = _Host(tmp_path)
    monkeypatch.setenv("PATH", f"{node.bin}:{os.environ.get('PATH', '')}")
    return node


def _dead_pid():
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def _members(pgid):
    """``{pid: command}`` of the live processes in group ``pgid``."""
    found = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat") as f:
                stat = f.read()
        except OSError:
            continue
        fields = stat[stat.rfind(")") + 2 :].split()
        if int(fields[2]) == pgid and fields[0] not in ("Z", "X"):
            found[int(name)] = stat[stat.find("(") + 1 : stat.rfind(")")]
    return found


def _wait_for(condition, what, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, what
        time.sleep(0.05)


@pytest.fixture
def run_group():
    """Start a process group like `runsc run`'s: a leader (sleep) and members
    running the given commands."""
    leaders = []

    def start(*commands):
        script = "".join(f"{command} & " for command in commands) + "exec sleep 300"
        leader = subprocess.Popen(["bash", "-c", script], start_new_session=True)
        leaders.append(leader)
        _wait_for(
            lambda: len(_members(leader.pid)) == len(commands) + 1
            and "bash" not in _members(leader.pid).values(),
            "the group never started",
        )
        return leader

    yield start
    for leader in leaders:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        leader.wait()


def _mount_dir(host, exits=True):
    """A bucket mount directory as a running sandbox leaves it: detached, its
    mount-s3 serving the container. This mount-s3 exits by itself once the
    container is deleted, as the real one does -- or, if not ``exits``, never.
    """
    parent = host.mounts / f"{os.getpid()}-{SANDBOX_ID}-x1y2"
    for name in ("0", "0-home"):
        (parent / name).mkdir(parents=True)
    (parent / "owner").write_text("{}")
    (parent / "0.log").write_text("")
    code = (
        "import os, time\n"
        "while not os.path.exists(os.environ['GONE']): time.sleep(0.02)\n"
        if exits
        else "import time; time.sleep(300)\n"
    )
    process = subprocess.Popen(
        # argv[0] is "mount-s3" and the mountpoint is in its argv, as the
        # cmdline check expects.
        ["mount-s3", "-c", code, "--", "bucket", str(parent / "0")],
        executable=sys.executable,
        env={**os.environ, "GONE": str(host.bin / "container-gone")},
        start_new_session=True,
    )
    (parent / "0.pid").write_text(str(process.pid))
    return parent, process


# -- teardown_sandbox ---------------------------------------------------------------


def test_teardown_removes_everything_in_order(host, run_group, monkeypatch):
    """The container first -- its end is what lets mount-s3 exit by itself,
    which it gets time to do -- then its process group, its mounts, its
    directory, and its image pin; another sandbox's pin stays."""
    root = host.make_sandbox()
    other_pin = host.pin(OTHER_ID)
    leader = run_group("sleep 301")
    teardown.write_run_group(str(root), leader.pid)
    members = _members(leader.pid)
    parent, mount_s3 = _mount_dir(host)

    order = []
    for name in ("_run_runsc", "_kill_run_group", "_reclaim", "_remove_sandbox_dir"):
        real = getattr(teardown, name)

        def record(*args, _name=name, _real=real, **kwargs):
            order.append(_name if _name != "_run_runsc" else args[1][0])
            return _real(*args, **kwargs)

        monkeypatch.setattr(teardown, name, record)

    assert host.teardown() is True
    assert order == [
        "kill",
        "delete",
        "_kill_run_group",
        "_reclaim",
        "_remove_sandbox_dir",
    ]
    assert host.calls() == [
        f"--root {host.runsc_root} kill {SANDBOX_ID} SIGKILL",
        f"--root {host.runsc_root} delete -force {SANDBOX_ID}",
    ]
    leader.wait(5)
    assert not any(teardown._process_alive(pid) for pid in members)
    assert mount_s3.wait(5) == 0, "exited by itself, within its grace"
    assert not parent.exists()
    assert not root.exists()
    assert glob.glob(f"{host.images}/*/.users/*") == [str(other_pin)]


def test_a_mount_s3_that_does_not_exit_is_killed_after_its_grace(host):
    host.make_sandbox()
    parent, mount_s3 = _mount_dir(host, exits=False)
    started = time.monotonic()
    host.teardown(grace_s=0.5)
    assert mount_s3.wait(5) == -signal.SIGKILL
    assert time.monotonic() - started >= 0.5
    assert not parent.exists()


def test_teardown_is_idempotent(host):
    root = host.make_sandbox()
    assert host.teardown() is True
    assert host.teardown() is True
    assert not root.exists()
    assert len(host.calls()) == 4


def test_a_failing_step_skips_none_of_the_rest(host, monkeypatch, caplog):
    root = host.make_sandbox()

    def wedged(*args, **kwargs):
        raise subprocess.TimeoutExpired("runsc", 5)

    monkeypatch.setattr(teardown, "_run_runsc", wedged)
    assert host.teardown() is True
    assert "Failed to kill its container" in caplog.text
    assert "Failed to delete its container" in caplog.text
    assert not root.exists()
    assert glob.glob(f"{host.images}/*/.users/*") == []


def test_another_reaper_holding_the_sandbox_is_left_to_it(host):
    root = host.make_sandbox()
    with open(root / "owner.json") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        assert host.teardown() is False
    assert root.exists()
    assert host.calls() == []


@pytest.mark.parametrize("case", ["a stranger in the group", "a group from before"])
def test_only_the_run_groups_own_processes_are_killed(host, run_group, case):
    """Its id and start time come from a file, so every member is checked: a
    command a sandbox's group does not run survives, and so does a group whose
    processes predate the record (the id was reused)."""
    root = host.make_sandbox()
    leader = run_group("sleep 301", "tail -f /dev/null")
    teardown.write_run_group(str(root), leader.pid)
    if case == "a group from before":
        record = json.loads((root / "run.pgid").read_text())
        record["start_time"] += 10**9
        (root / "run.pgid").write_text(json.dumps(record))
    host.teardown()
    survivors = sorted(_members(leader.pid).values())
    if case == "a stranger in the group":
        assert survivors == ["tail"]
    else:
        assert survivors == ["sleep", "sleep", "tail"]


@pytest.mark.parametrize(
    "sandbox_id",
    ["", "../etc", "ray-sandbox-0123", f"{SANDBOX_ID}/..", "ray-sandbox-*"],
)
def test_teardown_refuses_what_is_not_a_sandbox_id(host, sandbox_id):
    with pytest.raises(ValueError, match="Not a sandbox id"):
        host.teardown(sandbox_id)
    assert host.calls() == []


# -- sweep_orphaned_sandboxes --------------------------------------------------------


def test_the_sweep_tears_down_a_sandbox_whose_worker_and_supervisor_are_gone(host):
    root = host.make_sandbox(
        pid=_dead_pid(),
        supervisor_pid=_dead_pid(),
        supervisor_start_time=1,
        rootless=True,
        ignore_cgroups=True,
    )
    assert host.sweep() == [SANDBOX_ID]
    assert host.calls()[0] == (
        f"--rootless --ignore-cgroups --root {host.runsc_root} "
        f"kill {SANDBOX_ID} SIGKILL"
    )
    assert not root.exists()
    assert glob.glob(f"{host.images}/*/.users/*") == []


@pytest.mark.parametrize(
    "case",
    [
        "live owner",
        "live supervisor",
        "no owner.json",
        "other pid namespace",
        "not a sandbox id",
        "unreadable owner.json",
    ],
)
def test_the_sweep_leaves_alone_what_it_must(host, case):
    ppid = os.getppid()
    dead = {"pid": _dead_pid(), "supervisor_pid": _dead_pid()}
    owner = {
        "live owner": {"pid": ppid, "start_time": teardown._proc_start_time(ppid)},
        "live supervisor": {
            "pid": dead["pid"],
            "supervisor_pid": ppid,
            "supervisor_start_time": teardown._proc_start_time(ppid),
        },
        "other pid namespace": {**dead, "pid_ns": -1},
    }.get(case, dead)
    sandbox_id = (
        "ray-sandbox-not-hex-0000" if case == "not a sandbox id" else SANDBOX_ID
    )
    root = host.make_sandbox(sandbox_id, **owner)
    if case == "no owner.json":
        (root / "owner.json").unlink()
    elif case == "unreadable owner.json":
        (root / "owner.json").write_text("[not an object")
    assert host.sweep() == []
    assert root.exists()
    assert host.calls() == []


def test_nothing_in_owner_json_is_ever_run(host, tmp_path):
    """The flags are booleans, so a file cannot choose runsc's arguments, let
    alone a command; anything but ``true`` selects no flag."""
    marker = tmp_path / "PWNED"
    root = host.make_sandbox(
        pid=_dead_pid(),
        supervisor_pid=_dead_pid(),
        rootless=f"--rootless; touch {marker}",
        ignore_cgroups=["--ignore-cgroups"],
        runsc=["sh", "-c", f"touch {marker}"],
        argv=["sh", "-c", f"touch {marker}"],
    )
    assert host.sweep() == [SANDBOX_ID]
    assert host.calls()[0] == f"--root {host.runsc_root} kill {SANDBOX_ID} SIGKILL"
    assert not marker.exists()
    assert not root.exists()


# -- The supervisor process -----------------------------------------------------------


def _wait_torn_down(host, root, supervisor=None):
    _wait_for(lambda: not root.exists(), "the sandbox was never torn down")
    if supervisor is not None:
        _wait_for(lambda: not supervisor.alive(), "the supervisor did not exit")
    assert host.calls()[:2] == [
        f"--root {host.runsc_root} kill {SANDBOX_ID} SIGKILL",
        f"--root {host.runsc_root} delete -force {SANDBOX_ID}",
    ]


def test_the_supervisor_tears_down_when_the_lifeline_closes(host):
    root = host.make_sandbox()
    supervisor = host.supervisor()
    pid, start_time = supervisor.wait_ready()
    assert (pid, start_time) == (supervisor.pid, teardown._proc_start_time(pid))
    assert supervisor.alive()
    time.sleep(0.2)
    assert root.exists(), "nothing happens while the lifeline is open"
    supervisor.hand_over()
    _wait_torn_down(host, root, supervisor)
    assert glob.glob(f"{host.images}/*/.users/*") == []


def test_a_disarmed_supervisor_does_nothing(host):
    root = host.make_sandbox()
    supervisor = host.supervisor()
    supervisor.wait_ready()
    supervisor.disarm()
    assert supervisor._keeper.returncode == 0, "the keeper reaped it and exited"
    assert not supervisor.alive()
    supervisor.disarm()  # idempotent
    supervisor.hand_over()  # and final
    time.sleep(0.3)
    assert root.exists()
    assert host.calls() == []


def test_the_supervisor_outlives_its_keeper(host):
    """Ray SIGKILLs the direct children of a worker it kills: the keeper."""
    root = host.make_sandbox()
    supervisor = host.supervisor()
    supervisor.wait_ready()
    os.kill(supervisor._keeper.pid, signal.SIGKILL)
    supervisor._keeper.wait()
    assert supervisor.alive()
    supervisor.hand_over()
    _wait_torn_down(host, root, supervisor)


def test_the_supervisor_tears_down_when_its_worker_is_killed(host):
    """The worker's death closes the lifeline, whatever killed it."""
    root = host.make_sandbox()
    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import importlib.util, time
                spec = importlib.util.spec_from_file_location(
                    "teardown", {teardown.__file__!r})
                teardown = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(teardown)
                supervisor = teardown.SandboxSupervisor.start(
                    {SANDBOX_ID!r}, {host.runsc_args!r}, {str(host.sandboxes)!r},
                    {str(host.mounts)!r}, {str(host.images)!r})
                print(*supervisor.wait_ready(), flush=True)
                time.sleep(600)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    pid, start_time = (int(part) for part in worker.stdout.readline().split())
    worker.kill()
    worker.wait()
    _wait_torn_down(host, root)
    _wait_for(
        lambda: not teardown._process_alive(pid, start_time),
        "the supervisor did not exit",
    )


def test_the_supervisor_never_imports_ray():
    """It runs ``python -I teardown.py``: no ray, so no Ray state to fork."""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import runpy, sys; runpy.run_path(sys.argv[1]); "
            "print(sorted(m for m in sys.modules if m.split('.')[0] == 'ray'))",
            teardown.__file__,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "[]"


def test_wait_ready_reports_a_supervisor_that_never_started(host, monkeypatch):
    monkeypatch.setattr(teardown.sys, "executable", "/bin/false")
    supervisor = host.supervisor()
    with pytest.raises(RuntimeError, match="exited before it started"):
        supervisor.wait_ready()
    supervisor.disarm()


# -- A real sandbox whose owner is killed ----------------------------------------------


@pytest.mark.parametrize("network", ["none", "public"])
def test_a_killed_owner_leaves_nothing_behind(request, network):
    """No Python teardown runs in the owner, yet the container, the
    network="public" helpers, the directory and the image pin all go."""
    from ray.experimental.sandbox._internal.image_utils import DEFAULT_IMAGES_DIR
    from ray.experimental.sandbox.backend import gvisor

    if network == "public":
        request.getfixturevalue("ensure_slirp4netns")
    owner = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import time
                from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
                from ray.experimental.sandbox.config import GVisorSandboxConfig
                backend = GVisorSandboxBackend()
                print(backend.create_sandbox(GVisorSandboxConfig(
                    image="busybox:latest", shell="/bin/sh", network={network!r})),
                    flush=True)
                time.sleep(600)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    sandbox_id = owner.stdout.readline().strip()
    assert sandbox_id, "the owner failed to create its sandbox"
    root = os.path.join(gvisor._RAY_SANDBOX_DIR, sandbox_id)
    record = teardown.read_owner(root)
    with open(os.path.join(root, "run.pgid")) as f:
        group = json.load(f)["pgid"]
    state = teardown.runsc_base_args(
        record["rootless"], record["ignore_cgroups"], gvisor._RUNSC_ROOT
    ) + ["state", sandbox_id]
    pins = glob.glob(f"{DEFAULT_IMAGES_DIR}/*/.users/{sandbox_id}")
    assert pins
    assert subprocess.run(state, capture_output=True).returncode == 0
    assert _members(group)

    owner.kill()
    owner.wait()
    _wait_for(lambda: not os.path.exists(root), "the sandbox dir is still there", 30)
    _wait_for(
        lambda: not teardown._process_alive(
            record["supervisor_pid"], record["supervisor_start_time"]
        ),
        "the supervisor did not exit",
        30,
    )
    assert subprocess.run(state, capture_output=True).returncode != 0
    # SIGKILLed already; slirp4netns can take a moment in the kernel (state D)
    # to finish exiting.
    _wait_for(lambda: _members(group) == {}, f"left in the group: {_members(group)}")
    assert not any(os.path.exists(pin) for pin in pins)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
