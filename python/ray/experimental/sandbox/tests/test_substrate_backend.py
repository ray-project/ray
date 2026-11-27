"""Tests of the Substrate sandbox backend against a fake client.

No cluster and no ate-env-rl SDK needed: ``substrate_fake.py`` (loaded by
path, here and in the processes the tests start) stands in for ate-env-api.
"""

import importlib.util
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.backend import registry
from ray.experimental.sandbox.backend import substrate as sb
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.checkpoint_utils import (
    CheckpointPath,
    load_checkpoint_manifest,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime

FAKE_PATH = Path(__file__).resolve().with_name("substrate_fake.py")
FACTORY = f"{FAKE_PATH}:fake_client_factory"
IMAGE = "example.com/repo/img@sha256:" + "ab" * 32
READ_ONE_CMD = 'n=$(wc -c < /etc/passwd) && test "$n" -gt 0 && echo "$n"'


def _load_fake():
    spec = importlib.util.spec_from_file_location("substrate_fake_for_tests", FAKE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fake_mod = _load_fake()


@pytest.fixture
def fake(tmp_path, monkeypatch):
    for k in list(os.environ):
        if k.startswith("RAY_SUBSTRATE_") or k.startswith("SUBSTRATE_"):
            monkeypatch.delenv(k)
    # The backend shares one client per process; give each test a fresh one.
    sb.shutdown()
    sb._init_process_state()
    root = tmp_path / "fake"
    monkeypatch.setenv("SUBSTRATE_FAKE_ROOT", str(root))
    monkeypatch.setenv("RAY_SUBSTRATE_CLIENT_FACTORY", FACTORY)
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, "substrate")
    monkeypatch.setenv("NODE_NAME", "node-b")
    monkeypatch.setenv("RAY_SUBSTRATE_DOMAIN_BY_NODE", "node-a=d0,node-b=d70")
    monkeypatch.setenv(
        "RAY_SUBSTRATE_TEMPLATE_BY_DOMAIN", "0-65:tmpl-c3,66-74:tmpl-n2,75-120:tmpl-n2d"
    )
    yield fake_mod.FakeClient(root)
    sb.shutdown()


def _wait_for(pred, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not pred():
        time.sleep(0.02)
    assert pred()


def _checkpointed(rt, tmp_path, leave_running=False):
    sid = rt.create(image=IMAGE)
    ck = rt.checkpoint(
        sid, checkpoint_path=str(tmp_path / "ck"), leave_running=leave_running
    )
    return sid, ck


# --- configuration ---


def test_parse_template_by_domain():
    table = sb.parse_template_by_domain("0-65:a,66-74:b,75-120:c")
    assert table == [(0, 65, "a"), (66, 74, "b"), (75, 120, "c")]
    assert sb.template_for_domain(table, "d66", "x") == "b"
    assert sb.template_for_domain(table, "d121", "x") == "x"


def test_parse_domain_by_node(tmp_path):
    assert sb.parse_domain_by_node("a=d0, b=d1") == {"a": "d0", "b": "d1"}
    assert sb.parse_domain_by_node('{"a": "d0"}') == {"a": "d0"}
    f = tmp_path / "map.txt"
    f.write_text("# comment\nnode-x=d5\nnode-y=d6\n")
    assert sb.parse_domain_by_node(f"@{f}") == {"node-x": "d5", "node-y": "d6"}
    s = sb.SubstrateSettings.from_env(
        {"RAY_SUBSTRATE_DOMAIN_BY_NODE_FILE": str(f), "NODE_NAME": "node-y"}
    )
    assert s.placement() == ({"ate-env/domain": "d6"}, None)
    with pytest.raises(ValueError):
        sb.parse_domain_by_node("nonsense")


def test_placement():
    env = {
        "NODE_NAME": "n2",
        "RAY_SUBSTRATE_DOMAIN_BY_NODE": "n1=d3,n2=d80",
        "RAY_SUBSTRATE_TEMPLATE_BY_DOMAIN": "0-65:c3,66-120:n2d",
        "RAY_SUBSTRATE_TEMPLATE": "fallback",
    }
    from_env = sb.SubstrateSettings.from_env
    assert from_env(env).placement() == ({"ate-env/domain": "d80"}, "n2d")
    assert from_env({**env, "RAY_SUBSTRATE_DOMAIN": "d1"}).placement() == (
        {"ate-env/domain": "d1"},
        "c3",
    )
    assert from_env({**env, "RAY_SUBSTRATE_WORKER_SELECTOR": "p=x"}).placement() == (
        {"p": "x"},
        "fallback",
    )
    assert from_env({}).placement() == (None, None)
    with pytest.raises(SandboxCreationError, match="not in"):
        from_env({**env, "NODE_NAME": "n9"}).placement()
    with pytest.raises(SandboxCreationError, match="NODE_NAME"):
        from_env({k: v for k, v in env.items() if k != "NODE_NAME"}).placement()


def test_settings_tokens_and_modes(tmp_path):
    from_env = sb.SubstrateSettings.from_env
    tokens = {"SUBSTRATE_ENV_AUTH_TOKEN": "t1", "RAY_SUBSTRATE_AUTH_TOKEN": "t0"}
    assert from_env(tokens).auth_token == "t0"
    f = tmp_path / "token"
    f.write_text("from-file\n")
    assert (
        from_env({**tokens, "RAY_SUBSTRATE_AUTH_TOKEN_FILE": str(f)}).auth_token
        == "from-file"
    )
    assert from_env({}).restore_mode == "copy"
    assert from_env({"RAY_SUBSTRATE_RESTORE_MODE": "RESUME"}).restore_mode == "resume"
    with pytest.raises(ValueError, match="RESTORE_MODE"):
        from_env({"RAY_SUBSTRATE_RESTORE_MODE": "fork"})
    with pytest.raises(ValueError, match="SUSPEND_MODE"):
        from_env({"RAY_SUBSTRATE_SUSPEND_MODE": "async"})
    assert from_env({"RAY_SUBSTRATE_RPC_TIMEOUT_S": "7"}).rpc_timeout == 7.0


def test_command_line():
    assert sb._command_line(["/bin/sh", "-c", "echo hi"], "/bin/bash") == "echo hi"
    assert sb._command_line(["ls", "a b"], "/bin/bash") == "ls 'a b'"
    assert sb._command_line("echo hi", "/bin/bash") == "/bin/bash -c 'echo hi'"


def test_status_mapping():
    assert sb._sandbox_status(2) == SandboxStatus.RUNNING
    assert sb._sandbox_status(SimpleNamespace(name="PAUSED")) == SandboxStatus.PAUSED
    assert sb._sandbox_status(4) == SandboxStatus.PAUSED
    assert sb._sandbox_status(7) == SandboxStatus.ERROR


def test_handle_round_trip():
    handle = sb._make_handle("space", "env-1", {"env": {"A": "1"}, "shell": "/bin/sh"})
    assert handle.startswith("substrate://space/env-1?c=")
    parsed = sb._parse_handle(handle)
    assert parsed["env_id"] == "env-1" and parsed["atespace"] == "space"
    assert parsed["config"] == {"env": {"A": "1"}, "shell": "/bin/sh"}
    with pytest.raises(SandboxError, match="Malformed"):
        sb._parse_handle("substrate://nothing")


# --- selection, image manager, optional SDK ---


def test_env_var_selects_substrate_without_an_image_manager(fake, monkeypatch):
    built = []
    monkeypatch.setattr(
        "ray.experimental.sandbox.runtime.ImageManager",
        lambda: built.append(1) or object(),
    )
    rt = SandboxRuntime()
    assert isinstance(rt.backend, sb.SubstrateSandboxBackend)
    assert not built
    assert rt.image_manager.pull_image(IMAGE) == IMAGE


def test_sdk_missing_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("RAY_SUBSTRATE_CLIENT_FACTORY", raising=False)
    monkeypatch.setattr(sb.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError, match="pip install ate-env-rl"):
        SandboxRuntime(backend="substrate")


def test_gvisor_default_does_not_import_the_sdk_or_backend():
    code = (
        "import sys, os\n"
        "os.environ.pop('RAY_SANDBOX_BACKEND', None)\n"
        "import ray.experimental.sandbox\n"
        "from ray.experimental.sandbox.runtime import SandboxRuntime\n"
        "from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend\n"
        "assert isinstance(SandboxRuntime().backend, GVisorSandboxBackend)\n"
        "bad = [m for m in sys.modules if m.startswith('ate_env_rl')\n"
        "       or m == 'ray.experimental.sandbox.backend.substrate']\n"
        "assert not bad, bad\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)


def test_server_without_fork(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_NO_FORK_FEATURE", "1")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    with pytest.raises(SandboxError, match="fork_environment"):
        rt.restore(ck)
    assert rt.restore(ck, resume_in_place=True) == sid


def test_fork_check_runs_once_per_client(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    for _ in range(3):
        rt.restore(ck)
    assert len(fake.ops("server_info")) == 1


def test_sdk_without_fork(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_FORKLESS", "1")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    with pytest.raises(SandboxError, match="no Client.fork"):
        rt.restore(ck)
    with pytest.raises(SandboxError, match="no Client.fork"):
        rt.checkpoint(rt.create(image=IMAGE), leave_running=True)
    assert rt.restore(ck, resume_in_place=True) == sid


# --- options ---


@pytest.mark.parametrize(
    "option",
    [
        {"mounts": {"/data": "/data"}},
        {"cidr_allowlist": ["10.0.0.0/8"], "network": "public"},
        {"dns": ["8.8.8.8"], "network": "public"},
        {"capabilities": []},
        {"disk": "10Gi"},
        {"oom_score_adj": 100},
        {"network": "public"},
        {"network": "host"},
    ],
)
def test_unsupported_options_fail_closed(fake, option):
    with pytest.raises(SandboxCreationError, match="can't honor"):
        SandboxRuntime().create(image=IMAGE, **option)
    assert not fake.ops("create")


def test_docker_default_capabilities_count_as_unset(fake):
    from ray.experimental.sandbox.config import DOCKER_DEFAULT_CAPABILITIES

    rt = SandboxRuntime()
    caps = list(reversed(DOCKER_DEFAULT_CAPABILITIES))
    assert rt.exec(rt.create(image=IMAGE, capabilities=caps), "true").exit_code == 0
    with pytest.raises(SandboxCreationError, match="capabilities"):
        rt.create(image=IMAGE, capabilities=caps + ["SYS_ADMIN"])


def test_sandbox_host_creates_on_substrate(fake):
    import asyncio

    host_mod = pytest.importorskip("ray.experimental.sandbox.http.host")
    host = host_mod.SandboxHost(
        sandbox_id="sb-substrate0001",
        spec={
            "image": IMAGE,
            "env": {"A": "1"},
            "network": "none",
            "rootless": True,
            "cpu_limit": 64.0,  # no local cgroup check on this backend
            "memory_limit_mb": 1024,
        },
        settings={},
    )

    async def boot():
        await host.boot()
        return await host.describe()

    info = asyncio.run(boot())
    assert info["status"] == "running", info["error"]
    (create,) = fake.ops("create")
    assert create["image"] == IMAGE


def test_ignored_options_are_accepted(fake):
    rt = SandboxRuntime()
    sid = rt.create(
        image=IMAGE,
        cpu=2.0,
        memory="4Gi",
        rootless=False,
        network="none",
        _oci_spec_transform_fn=lambda spec: spec,
    )
    assert rt.exec(sid, ["/bin/sh", "-c", "true"]).exit_code == 0


def test_string_commands_use_the_configured_shell(fake):
    rt = SandboxRuntime()
    default = rt.create(image=IMAGE)
    assert rt.exec(default, "echo $0").stdout.strip() == "/bin/bash"
    custom = rt.create(image=IMAGE, shell="/bin/sh")
    assert rt.exec(custom, "echo $0").stdout.strip() == "/bin/sh"
    assert rt.exec(custom, "echo $0", shell="/bin/bash").stdout.strip() == "/bin/bash"


# --- checkpoints and restores ---


def test_restore_copies_by_default(fake, tmp_path):
    rt = SandboxRuntime()
    sid = rt.create(
        image=IMAGE, cpu=2.0, memory="4Gi", env={"FOO": "bar"}, workdir="/tmp"
    )
    (create,) = fake.ops("create")
    assert create["selector"] == {"ate-env/domain": "d70"}
    assert create["template"] == "tmpl-n2" and create["image"] == IMAGE
    rt.write_file(sid, "/tmp/state", "step1")
    ck = rt.checkpoint(sid, checkpoint_path=str(tmp_path / "ck"), leave_running=False)
    assert isinstance(ck, CheckpointPath) and str(ck) == str(tmp_path / "ck")
    assert ck.remote is False and ck.cpu == 2.0 and ck.memory == "4Gi"
    assert fake.ops("suspend")[0]["mode"] == 2  # LOCAL

    # The checkpoint owns the sandbox now.
    with pytest.raises(SandboxNotFoundError, match="belongs to its checkpoint"):
        rt.exec(sid, "true")
    rt.delete(sid)
    assert sid in fake.live()

    copies = [rt.restore(ck) for _ in range(3)]
    assert len(set(copies)) == 3 and sid not in copies
    for c in copies:
        assert rt.read_file(c, "/tmp/state") == b"step1"
        assert rt.exec(c, "echo $FOO").stdout.strip() == "bar"
    assert fake.ops("fork")[0]["selector"] == {"ate-env/domain": "d70"}
    rt.write_file(copies[0], "/tmp/state", "changed")
    assert rt.read_file(rt.restore(ck), "/tmp/state") == b"step1"
    rt.delete(copies[0])  # only that copy
    assert {sid, *copies[1:]} <= fake.live() and copies[0] not in fake.live()

    rt.delete_checkpoint(ck)
    assert sid not in fake.live()
    assert not os.path.exists(ck)  # files too
    with pytest.raises(SandboxError, match="No checkpoint"):
        rt.restore(ck)


def test_leave_running_checkpoint_is_a_snapshot(fake, tmp_path):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE)
    rt.write_file(sid, "/tmp/state", "before")
    ck = rt.checkpoint(sid, checkpoint_path=str(tmp_path / "ck"), leave_running=True)
    assert not fake.ops("suspend")  # the copy is born suspended
    rt.write_file(sid, "/tmp/state", "after")  # the sandbox keeps running
    assert rt.read_file(rt.restore(ck), "/tmp/state") == b"before"
    resumed = rt.restore(ck, resume_in_place=True)
    assert resumed != sid
    assert rt.read_file(resumed, "/tmp/state") == b"before"
    rt.delete(sid)
    assert rt.read_file(resumed, "/tmp/state") == b"before"


def test_resume_in_place_is_single_use_and_rpc_free(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    n_ops = len(fake.ops())
    assert rt.restore(ck, resume_in_place=True) == sid
    assert len(fake.ops()) == n_ops  # woken by its next call
    with pytest.raises(SandboxError, match="already resumed"):
        rt.restore(ck, resume_in_place=True)
    with pytest.raises(SandboxError, match="already resumed"):
        rt.restore(ck)
    assert rt.exec(sid, "true").exit_code == 0  # usable again
    assert rt.get_status(sid) == SandboxStatus.RUNNING
    rt.checkpoint(sid, checkpoint_path=str(ck), leave_running=False)
    assert rt.restore(ck, resume_in_place=True) == sid
    rt.delete(sid)
    assert rt.get_status(sid) == SandboxStatus.TERMINATED


def test_restore_mode_setting(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("RAY_SUBSTRATE_RESTORE_MODE", "resume")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    assert rt.restore(ck) == sid
    assert not fake.ops("fork")


def test_new_checkpoint_at_a_path_replaces_the_old(fake, tmp_path):
    rt = SandboxRuntime()
    first, ck = _checkpointed(rt, tmp_path)
    second = rt.create(image=IMAGE)
    rt.checkpoint(second, checkpoint_path=str(ck), leave_running=False)
    _wait_for(lambda: first not in fake.live())
    assert rt.restore(ck, resume_in_place=True) == second


def test_manifest_for_top_level_restore(fake, tmp_path):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE, cpu=1.0, memory="1Gi")
    ck = rt.checkpoint(
        sid, checkpoint_path=str(tmp_path / "ck"), leave_running=False, remote=True
    )
    manifest, state_dir = load_checkpoint_manifest(str(ck))
    assert os.path.isdir(state_dir)
    assert manifest["remote"] is True and ck.remote is True
    assert manifest["config"]["cpu"] == 1.0 and manifest["config"]["memory"] == "1Gi"
    assert manifest["backend"] == "substrate"
    assert manifest["substrate"]["env_id"] == sid
    assert ck.node_id == manifest["node_id"]
    # restore ignores remote; it works on any node that can read the path.
    assert rt.restore(str(ck), remote=False) != sid


def test_handle_checkpoint(fake):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE, env={"FOO": "bar"}, shell="/bin/sh")
    handle = rt.checkpoint(sid, leave_running=False)
    assert isinstance(handle, CheckpointPath) and handle.remote is True
    assert handle.startswith(f"substrate://ate-env/{sid}?c=")
    sb._meta.clear()  # as in another process: only the handle is left
    sb._retired.clear()
    copy = rt.restore(handle)
    assert rt.exec(copy, "echo $FOO $0").stdout.split() == ["bar", "/bin/sh"]
    assert rt.restore(handle, resume_in_place=True) == sid
    with pytest.raises(SandboxError, match="already resumed"):
        rt.restore(handle)
    other = rt.checkpoint(rt.create(image=IMAGE), leave_running=False)
    rt.delete_checkpoint(other)
    with pytest.raises(SandboxError, match="was deleted"):
        rt.restore(other)


def test_restore_options(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    with pytest.raises(SandboxError, match="can't honor"):
        rt.restore(ck, dns=["8.8.8.8"])
    with pytest.raises(SandboxError, match="of image"):
        rt.restore(ck, image="other@sha256:" + "cd" * 32)
    copy = rt.restore(ck, env={"EXTRA": "1"}, _oci_spec_transform_fn=lambda s: s)
    assert rt.exec(copy, "echo $EXTRA").stdout.strip() == "1"


def test_restore_errors(fake, tmp_path):
    rt = SandboxRuntime()
    with pytest.raises(SandboxError, match="No checkpoint"):
        rt.restore(str(tmp_path / "nope"))
    other = tmp_path / "other"
    other.mkdir()
    (other / "manifest.json").write_text('{"version": "1.0", "backend": "substrate"}')
    with pytest.raises(SandboxError, match="not a Substrate checkpoint"):
        rt.restore(str(other))


def test_ttl_timer_is_replaced_on_resume_in_place(fake, tmp_path):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE, ttl_seconds=3600)
    ck = rt.checkpoint(sid, checkpoint_path=str(tmp_path / "ck"), leave_running=False)
    assert sid not in rt._ttl_timers  # checkpoint(leave_running=False) drops it
    assert rt.restore(ck, resume_in_place=True, ttl_seconds=7200) == sid
    assert rt._ttl_timers[sid].interval == 7200
    rt.delete(sid)


def test_files_env_timeout_and_errors(fake):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE, env={"FOO": "bar"})
    rt.write_file(sid, "/tmp/a", "hello")
    rt.write_file(sid, "/tmp/a", b" world", append=True)
    assert rt.read_file(sid, "/tmp/a") == b"hello world"
    assert rt.exec(sid, "echo $FOO").stdout.strip() == "bar"
    assert rt.exec(sid, "exit 3").exit_code == 3
    with pytest.raises(SandboxTimeoutError):
        rt.exec(sid, "sleep 5", timeout=0.2)
    rt.pause(sid)
    assert rt.get_status(sid) == SandboxStatus.PAUSED
    rt.resume(sid)
    assert rt.exec(sid, "true").exit_code == 0
    rt.delete(sid)
    with pytest.raises(SandboxError):
        rt.exec(sid, "true")
    with pytest.raises(SandboxCreationError, match="pinned"):
        rt.create(image="busybox:latest")


def test_unknown_node(fake, monkeypatch):
    monkeypatch.setenv("NODE_NAME", "elsewhere")
    with pytest.raises(SandboxCreationError, match="elsewhere"):
        SandboxRuntime().create(image=IMAGE)


def test_many_threads(fake, tmp_path, monkeypatch):
    n = 100
    barrier = threading.Barrier(n)
    errors, ids = [], []

    def worker(i):
        try:
            rt = SandboxRuntime()
            barrier.wait()
            sid = rt.create(image=IMAGE)
            ck = rt.checkpoint(
                sid, checkpoint_path=str(tmp_path / f"t{i}"), leave_running=False
            )
            assert rt.restore(ck, resume_in_place=True) == sid
            assert rt.exec(sid, f"echo {i}").stdout.strip() == str(i)
            rt.delete(sid)
            ids.append(sid)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    [t.start() for t in threads]
    [t.join(120) for t in threads]
    assert not errors, errors[:3]
    assert len(set(ids)) == n
    assert [t.name for t in threading.enumerate()].count("ray-substrate-backend") == 1


def test_concurrent_resumes_use_the_checkpoint_once(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    barrier = threading.Barrier(16)
    results, errors = [], []

    def go():
        barrier.wait()
        try:
            results.append(rt.restore(ck, resume_in_place=True))
        except SandboxError as e:
            errors.append(e)

    threads = [threading.Thread(target=go) for _ in range(16)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results == [sid] and len(errors) == 15


# --- concurrent copies serialize through ABORTED retries ---


def _copies_in_threads(rt, ck, n):
    barrier = threading.Barrier(n)
    out, errors = [], []

    def branch():
        try:
            barrier.wait()
            out.append(rt.restore(ck))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=branch) for _ in range(n)]
    [t.start() for t in threads]
    [t.join(60) for t in threads]
    assert not errors, errors
    return out


@pytest.mark.parametrize("handle", [False, True])
def test_concurrent_copies_from_threads_all_succeed(
    fake, tmp_path, monkeypatch, handle
):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "0.2")
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE)
    path = None if handle else str(tmp_path / "ck")
    ck = rt.checkpoint(sid, checkpoint_path=path, leave_running=False)
    out = _copies_in_threads(rt, ck, 6)
    forks = fake.ops("fork")
    assert len(set(out)) == 6
    assert len(forks) == 6 and all(f["count"] == 1 for f in forks)
    assert fake.ops("fork_aborted")  # they collided, and retried
    assert fake.live() == {sid, *out}


def test_aborted_until_the_deadline_is_a_clear_error(fake, tmp_path, monkeypatch):
    monkeypatch.setattr(sb, "_GRACE_S", 1.0)
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    op = Path(fake.root) / "ate-env" / f"{sid}.op"
    op.touch()  # another operation holds the source
    threading.Timer(0.3, op.unlink).start()
    copy = rt.restore(ck, timeout_seconds=1.0)  # waits it out
    assert fake.ops("fork_aborted") and len(fake.ops("fork")) == 1
    op.touch()  # held for good
    t0 = time.monotonic()
    with pytest.raises(SandboxError, match="stayed busy") as err:
        rt.restore(ck, timeout_seconds=1.0)
    assert "env-rl/issues/69" in str(err.value)
    # The 2 s budget, less the last attempt's minimum of 1 s.
    assert 0.5 <= time.monotonic() - t0 < 5
    assert len(fake.ops("fork_aborted")) > 2
    op.unlink()
    assert fake.live() == {sid, copy}


_CHILD = """
import os, sys, time
from ray.experimental.sandbox.runtime import SandboxRuntime
rt = SandboxRuntime()
ready, go = sys.argv[2], sys.argv[3]
open(ready, "w").close()
deadline = time.monotonic() + 60
while not os.path.exists(go) and time.monotonic() < deadline:
    time.sleep(0.005)
print(rt.restore(sys.argv[1]), flush=True)
"""


def test_concurrent_copies_from_processes_all_succeed(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "0.3")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    go = tmp_path / "go"
    readies = [tmp_path / f"ready{i}" for i in range(4)]
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _CHILD, str(ck), str(r), str(go)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for r in readies
    ]
    _wait_for(lambda: all(r.exists() for r in readies), timeout=120)
    go.touch()
    outs = []
    for p in procs:
        o, e = p.communicate(timeout=120)
        assert p.returncode == 0, e[-2000:]
        outs.append(o.strip().splitlines()[-1])
    assert len(set(outs)) == 4 and sid not in outs
    assert len(fake.ops("fork")) == 4
    assert fake.live() == {sid, *outs}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded")
def test_usable_after_os_fork(fake):
    rt = SandboxRuntime()
    assert rt.exec(rt.create(image=IMAGE), "true").exit_code == 0  # loop running
    pid = os.fork()
    if pid == 0:  # child
        code = 1
        try:
            for child_rt in (rt, SandboxRuntime()):  # built before and after
                sid = child_rt.create(image=IMAGE)
                if child_rt.exec(sid, "echo child").stdout.strip() != "child":
                    break
            else:
                code = 0
        finally:
            os._exit(code)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            break
        time.sleep(0.05)
    else:
        os.kill(pid, 9)
        pytest.fail("the forked child hung")
    assert os.waitstatus_to_exitcode(status) == 0
    assert rt.exec(rt.create(image=IMAGE), "true").exit_code == 0  # parent too


def test_shutdown_closes_and_restarts(fake):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE)
    client = rt.backend.client
    sb.shutdown()
    assert client.closed
    assert [t.name for t in threading.enumerate()].count("ray-substrate-backend") == 0
    assert SandboxRuntime().exec(sid, "true").exit_code == 0


def test_rpc_timeout_bounds_control_calls_not_exec(fake, monkeypatch):
    import asyncio

    monkeypatch.setenv("RAY_SUBSTRATE_RPC_TIMEOUT_S", "0.3")
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE)
    assert rt.exec(sid, "sleep 1 && echo done").stdout.strip() == "done"
    with pytest.raises(TimeoutError):
        rt.backend._run(asyncio.sleep(5))


def test_checkpoint_refuses_a_non_empty_dir(fake, tmp_path):
    rt = SandboxRuntime()
    sid = rt.create(image=IMAGE)
    target = tmp_path / "busy"
    target.mkdir()
    (target / "data.txt").write_text("keep me")
    with pytest.raises(SandboxError, match="not empty"):
        rt.checkpoint(sid, checkpoint_path=str(target), leave_running=True)
    assert (target / "data.txt").read_text() == "keep me"


def test_restore_refuses_a_gvisor_checkpoint(fake, tmp_path):
    ckpt = tmp_path / "gvisor-ckpt"
    (ckpt / "state").mkdir(parents=True)
    (ckpt / "manifest.json").write_text('{"version": "1.0"}')
    with pytest.raises(SandboxError, match="taken by the gvisor"):
        SandboxRuntime().restore(str(ckpt))


def test_bounded_dict_is_lru():
    d = sb._BoundedDict(cap=2)
    d["a"], d["b"] = 1, 2
    assert d.get("a") == 1  # refreshes a
    d["c"] = 3
    assert "a" in d and "b" not in d and "c" in d


def test_fork_timeout_deletes_the_late_copy(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "2.0")
    monkeypatch.setattr(sb, "_GRACE_S", 0.2)
    monkeypatch.setattr(sb, "_EVENTUAL_DELETE_FIRST_S", 0.25)
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    with pytest.raises(SandboxTimeoutError):
        rt.restore(ck, timeout_seconds=0.3)  # the client gives up mid-fork
    (fork,) = fake.ops("fork")
    # The server still makes the copy; the backend deletes it once it exists.
    _wait_for(lambda: fake.live() == {sid}, timeout=15)
    assert set(fork["ids"]) & {o["id"] for o in fake.ops("delete")}
    assert rt.restore(ck) not in fork["ids"]


def test_recheckpoint_during_copy(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "0.8")
    rt = SandboxRuntime()
    first = rt.create(image=IMAGE)
    rt.write_file(first, "/tmp/state", "first")
    ck = rt.checkpoint(first, checkpoint_path=str(tmp_path / "ck"), leave_running=False)
    out = []
    t = threading.Thread(target=lambda: out.append(rt.restore(ck)))
    t.start()
    time.sleep(0.2)  # the copy is forking `first`
    second = rt.create(image=IMAGE)
    rt.write_file(second, "/tmp/state", "second")
    rt.checkpoint(second, checkpoint_path=str(ck), leave_running=False)
    t.join(30)
    (copy,) = out  # of the old checkpoint, or of the new one if it was gone
    assert rt.read_file(copy, "/tmp/state") in (b"first", b"second")
    _wait_for(lambda: first not in fake.live())  # replaced: deleted
    assert rt.read_file(rt.restore(ck), "/tmp/state") == b"second"
    assert rt.restore(ck, resume_in_place=True) == second


def test_recheckpoint_and_resume_are_serialized(fake, tmp_path):
    rt = SandboxRuntime()
    first, ck = _checkpointed(rt, tmp_path)
    second = rt.create(image=IMAGE)
    results = []

    def resume():
        try:
            results.append(rt.restore(ck, resume_in_place=True))
        except SandboxError as e:
            results.append(e)

    with rt.backend._checkpoint_lock(str(ck)):  # as a checkpoint write would hold it
        t = threading.Thread(target=resume)
        t.start()
        time.sleep(0.2)
        assert not results  # the resume waits for the lock
    t.join(10)
    assert results == [first]
    rt.checkpoint(second, checkpoint_path=str(ck), leave_running=False)
    assert rt.restore(ck, resume_in_place=True) == second


def test_copy_waiting_on_aborted_stops_when_resumed_in_place(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    op = Path(fake.root) / "ate-env" / f"{sid}.op"
    op.touch()  # the source is busy: the copy retries ABORTED
    errors = []
    t = threading.Thread(target=lambda: _catch(errors, rt.restore, ck))
    t.start()
    time.sleep(0.3)
    assert rt.restore(ck, resume_in_place=True) == sid  # handed out
    op.unlink()
    t.join(30)
    assert len(errors) == 1 and "already resumed" in str(errors[0])
    assert not fake.ops("fork")  # never forked the live sandbox
    assert fake.live() == {sid}


def test_copy_resumed_in_place_during_its_fork_is_deleted(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "1.0")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    errors = []
    t = threading.Thread(target=lambda: _catch(errors, rt.restore, ck))
    t.start()
    time.sleep(0.3)  # the copy is forking
    assert rt.restore(ck, resume_in_place=True) == sid
    t.join(30)
    assert len(errors) == 1 and "used up while it was being copied" in str(errors[0])
    _wait_for(lambda: fake.live() == {sid})


def test_copy_during_resume_and_recheckpoint_of_the_same_env_is_deleted(
    fake, tmp_path, monkeypatch
):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "1.0")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    errors = []
    t = threading.Thread(target=lambda: _catch(errors, rt.restore, ck))
    t.start()
    time.sleep(0.3)  # the copy is forking
    assert rt.restore(ck, resume_in_place=True) == sid  # a step...
    rt.checkpoint(sid, checkpoint_path=str(ck), leave_running=False)  # ...done
    t.join(30)
    assert len(errors) == 1 and "checkpointed again" in str(errors[0])
    _wait_for(lambda: fake.live() == {sid})
    assert rt.restore(ck, resume_in_place=True) == sid  # the new one is intact


def test_delete_checkpoint_retries_while_a_copy_forks(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_FAKE_LATENCY_S", "1.0")
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    errors = []
    t = threading.Thread(target=lambda: _catch(errors, rt.restore, ck))
    t.start()
    time.sleep(0.3)  # the copy holds the source
    rt.delete_checkpoint(ck)  # ABORTED until the fork ends, then deleted
    t.join(30)
    assert fake.ops("delete_aborted")
    assert not os.path.exists(ck)
    assert len(errors) == 1  # the copy saw its checkpoint go
    _wait_for(lambda: fake.live() == set())


def test_delete_checkpoint_busy_past_its_budget(fake, tmp_path, monkeypatch):
    monkeypatch.setenv("RAY_SUBSTRATE_RPC_TIMEOUT_S", "0.8")
    monkeypatch.setattr(sb, "_EVENTUAL_DELETE_FIRST_S", 0.25)
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    op = Path(fake.root) / "ate-env" / f"{sid}.op"
    op.touch()
    threading.Timer(1.0, op.unlink).start()
    with pytest.raises(SandboxError, match="still retrying"):
        rt.delete_checkpoint(ck)
    assert not os.path.exists(ck)  # not left renamed but undeleted
    _wait_for(lambda: fake.live() == set(), timeout=15)


def test_replaced_checkpoint_env_is_deleted_once_free(fake, tmp_path):
    rt = SandboxRuntime()
    first, ck = _checkpointed(rt, tmp_path)
    op = Path(fake.root) / "ate-env" / f"{first}.op"
    op.touch()  # busy, as with a copy's fork
    threading.Timer(0.5, op.unlink).start()
    second = rt.create(image=IMAGE)
    rt.checkpoint(second, checkpoint_path=str(ck), leave_running=False)
    _wait_for(lambda: first not in fake.live())
    assert fake.ops("delete_aborted")


def _catch(errors, fn, *args):
    try:
        fn(*args)
    except SandboxError as e:
        errors.append(e)


def test_checkpoint_after_delete_tolerates_the_lock_file(fake, tmp_path):
    rt = SandboxRuntime()
    sid, ck = _checkpointed(rt, tmp_path)
    rt.delete_checkpoint(ck)
    os.makedirs(ck)
    Path(ck, sb.LOCK_FILE).touch()  # what a restore in flight leaves behind
    other = rt.create(image=IMAGE)
    rt.checkpoint(other, checkpoint_path=str(ck), leave_running=False)
    assert rt.restore(ck, resume_in_place=True) == other


def test_sandbox_actor_checkpoint_and_top_level_restore(fake, tmp_path):
    import ray
    import ray.experimental.sandbox as sandbox

    ray.init(num_cpus=4, include_dashboard=False)
    try:
        actor = sandbox.create(IMAGE, cpu=1, memory="256Mi")
        ray.get(actor.write_file.remote("/tmp/state", "kept"))
        for path in (str(tmp_path / "ck"), None):
            ck = ray.get(
                actor.checkpoint.remote(checkpoint_path=path, leave_running=True)
            )
            assert isinstance(ck, CheckpointPath)
            restored = sandbox.restore(ck)
            assert ray.get(restored.read_file.remote("/tmp/state")) == b"kept"
    finally:
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
