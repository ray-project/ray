import asyncio
import logging
import os
import platform
import sys
import time
from dataclasses import replace
from typing import List, Tuple

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private import ray_constants
from ray._private.runtime_env.agent.runtime_env_agent import ReferenceTable, UriType
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import (
    ImageMetadata,
    ImageURIPlugin,
    _canonical_pip_config,
    _check_host_compatibility,
    _get_image_uri_cache_key,
)
from ray._private.test_utils import (
    get_error_message,
    init_error_pubsub,
)
from ray.core.generated import common_pb2
from ray.runtime_env import RuntimeEnv

import psutil

logger = logging.getLogger(__name__)


def test_reference_table():
    expected_unused_uris = []
    expected_unused_runtime_env = str()

    def uris_parser(runtime_env) -> Tuple[str, UriType]:
        result = list()
        result.append((runtime_env.working_dir(), "working_dir"))
        py_module_uris = runtime_env.py_modules()
        for uri in py_module_uris:
            result.append((uri, "py_modules"))
        return result

    def unused_uris_processor(unused_uris: List[Tuple[str, UriType]]) -> None:
        nonlocal expected_unused_uris
        assert expected_unused_uris
        for unused in unused_uris:
            assert unused in expected_unused_uris
            expected_unused_uris.remove(unused)
        assert not expected_unused_uris

    def unused_runtime_env_processor(unused_runtime_env: str) -> None:
        nonlocal expected_unused_runtime_env
        assert expected_unused_runtime_env
        assert expected_unused_runtime_env == unused_runtime_env
        expected_unused_runtime_env = None

    reference_table = ReferenceTable(
        uris_parser, unused_uris_processor, unused_runtime_env_processor
    )
    runtime_env_1 = RuntimeEnv(
        working_dir="s3://working_dir_1.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_B.zip"],
    )
    runtime_env_2 = RuntimeEnv(
        working_dir="s3://working_dir_2.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_C.zip"],
    )
    # Add runtime env 1
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    # Add runtime env 2
    reference_table.increase_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    # Add runtime env 1 by `client_server`, this will be skipped by reference table.
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "client_server"
    )

    # Remove runtime env 1
    expected_unused_uris.append(("s3://working_dir_1.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_B.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_1.serialize()
    reference_table.decrease_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env

    # Remove runtime env 2
    expected_unused_uris.append(("s3://working_dir_2.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_A.zip", "py_modules"))
    expected_unused_uris.append(("s3://py_module_C.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_2.serialize()
    reference_table.decrease_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env


def test_reference_table_dynamic_uris():
    unused_uris = []
    unused_runtime_envs = []
    reference_table = ReferenceTable(
        lambda runtime_env: [],
        unused_uris.extend,
        unused_runtime_envs.append,
    )
    runtime_env = RuntimeEnv(image_uri="example/image:latest", pip=["package==1"])
    serialized_env = runtime_env.serialize()
    dynamic_uri = ("image-pip://cache-key", "image_uri")

    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    assert reference_table._uri_reference[dynamic_uri[0]] == 2

    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
    assert unused_uris == []
    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
    assert unused_uris == [dynamic_uri]
    assert unused_runtime_envs == [serialized_env]


def _image_metadata_for_test():
    return ImageMetadata(
        image_id="sha256:image",
        image_digest="sha256:digest",
        os="linux",
        architecture="amd64",
        python_executable="/usr/bin/python",
        python_version="3.12.1",
        python_implementation="CPython",
        python_cache_tag="cpython-312",
        python_soabi="cpython-312-x86_64-linux-gnu",
        python_platform="linux-x86_64",
        python_path="/usr/bin",
        ray_version="2.56.0",
        ray_commit="commit",
        worker_path="/ray/default_worker.py",
    )


def test_reference_table_releases_env_dereferenced_during_creation():
    unused_uris = []
    unused_runtime_envs = []
    reference_table = ReferenceTable(
        lambda runtime_env: [],
        unused_uris.extend,
        unused_runtime_envs.append,
    )
    runtime_env = RuntimeEnv(image_uri="example/image:latest", pip=["package==1"])
    serialized_env = runtime_env.serialize()
    dynamic_uri = ("image-pip://cache-key", "image_uri")

    # The delete arrives before the dynamic URI is resolved; creation then
    # finishes and must release everything it cached.
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    unused_uris.clear()
    unused_runtime_envs.clear()
    reference_table.release_dynamic_uris_if_unreferenced(serialized_env, "raylet")
    assert unused_uris == [dynamic_uri]
    assert unused_runtime_envs == [serialized_env]

    # No-op while references remain or for excluded sources.
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    unused_uris.clear()
    unused_runtime_envs.clear()
    reference_table.release_dynamic_uris_if_unreferenced(serialized_env, "raylet")
    reference_table.release_dynamic_uris_if_unreferenced(
        serialized_env, "client_server"
    )
    assert unused_uris == []
    assert unused_runtime_envs == []


def test_reference_table_releases_uris_bound_before_delete():
    unused_uris = []
    unused_runtime_envs = []
    reference_table = ReferenceTable(
        lambda runtime_env: [],
        unused_uris.extend,
        unused_runtime_envs.append,
    )
    runtime_env = RuntimeEnv(image_uri="example/image:latest", pip=["package==1"])
    serialized_env = runtime_env.serialize()
    dynamic_uri = ("image-pip://cache-key", "image_uri")

    # The delete arrives after the dynamic URI was bound but while the slow
    # install is still running: the delete pops the binding while the URI is
    # not yet in the cache, so the finished creation must pass the URIs it
    # resolved to get them released.
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
    unused_uris.clear()
    unused_runtime_envs.clear()
    reference_table.release_dynamic_uris_if_unreferenced(
        serialized_env, "raylet", [dynamic_uri]
    )
    assert unused_uris == [dynamic_uri]
    assert unused_runtime_envs == [serialized_env]


def test_image_uri_cache_key_covers_image_python_and_requirements():
    metadata = _image_metadata_for_test()
    pip_config = {
        "packages": ["requests==2.32.3"],
        "pip_check": False,
        "pip_version": None,
        "pip_install_options": ["--disable-pip-version-check"],
    }
    original = _get_image_uri_cache_key(metadata, pip_config, {})

    assert original != _get_image_uri_cache_key(
        replace(metadata, image_digest="sha256:other"), pip_config, {}
    )
    assert original != _get_image_uri_cache_key(
        replace(metadata, python_soabi="cpython-312-aarch64-linux-gnu"),
        pip_config,
        {},
    )
    assert original != _get_image_uri_cache_key(
        metadata,
        {**pip_config, "packages": ["requests==2.32.4"]},
        {},
    )
    assert original != _get_image_uri_cache_key(
        metadata, pip_config, {"PIP_INDEX_URL": "https://example.test/simple"}
    )


@pytest.mark.parametrize(
    ("path_env_vars", "expected_path"),
    [
        (
            {"PATH": "/runtime/bin"},
            os.pathsep.join(["/cache/virtualenv/bin", "/runtime/bin"]),
        ),
        ({}, os.pathsep.join(["/cache/virtualenv/bin", "/image/bin"])),
    ],
)
def test_runtime_env_context_execs_container_as_argv(
    monkeypatch, path_env_vars, expected_path
):
    executed = {}

    def execve(file, args, env):
        executed["file"] = file
        executed["args"] = args
        executed["env"] = env

    monkeypatch.setattr(os, "execve", execve)
    monkeypatch.setattr("shutil.which", lambda executable: "/usr/bin/podman")
    monkeypatch.setenv("RAY_JOB_ID", "job-id")
    context = RuntimeEnvContext(
        env_vars={**path_env_vars, "USER_VALUE": "contains spaces"},
        override_worker_entrypoint="/image/default_worker.py",
        container={
            "command": ["podman", "run", "--rm"],
            "image": "sha256:image",
            "entrypoint": "/cache/virtualenv/bin/python",
            "mounts": [
                {
                    "source": "/cache",
                    "target": "/cache",
                    "read_only": True,
                    "options": "z",
                }
            ],
            "env_vars": {"USER_VALUE": "contains spaces"},
            "path_prefix": "/cache/virtualenv/bin",
            "default_path": "/image/bin",
        },
    )

    context.exec_worker(
        ["/host/default_worker.py", "--arg", "value with spaces"],
        common_pb2.Language.PYTHON,
    )

    assert executed["file"] == "/usr/bin/podman"
    assert "USER_VALUE" in executed["args"]
    assert "USER_VALUE=contains spaces" not in executed["args"]
    assert executed["env"]["USER_VALUE"] == "contains spaces"
    assert executed["env"]["PATH"] == expected_path
    assert "/cache:/cache:ro,z" in executed["args"]
    assert executed["args"][-3:] == [
        "/image/default_worker.py",
        "--arg",
        "value with spaces",
    ]


@pytest.mark.asyncio
async def test_image_uri_uses_stdlib_venv(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://cache-key"
    metadata = _image_metadata_for_test()
    plugin._metadata_by_uri[uri] = metadata
    plugin._install_env_by_uri[uri] = {}
    calls = []

    async def run_in_image(
        metadata, staging_path, final_path, entrypoint, args, install_env, logger
    ):
        calls.append((entrypoint, args))
        if args[0] == "-c":
            return "RAY_VERSION=2.56.0\n"
        return ""

    monkeypatch.setattr(plugin, "_run_in_image", run_in_image)
    staging_path = str(tmp_path / "staging")
    final_path = str(tmp_path / "final")
    os.makedirs(staging_path)
    await plugin._prepare_pip_environment(
        uri,
        RuntimeEnv(image_uri="example/image", pip=["package==1"]),
        metadata,
        staging_path,
        final_path,
        logger,
    )

    assert calls[0] == (
        metadata.python_executable,
        [
            "-m",
            "venv",
            "--system-site-packages",
            "--without-pip",
            os.path.join(final_path, "virtualenv"),
        ],
    )


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cache_publishes_once(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://cache-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {}
    prepare_calls = 0

    async def prepare(*args, **kwargs):
        nonlocal prepare_calls
        prepare_calls += 1
        await asyncio.sleep(0)

    monkeypatch.setattr(plugin, "_prepare_pip_environment", prepare)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    context = RuntimeEnvContext()
    await asyncio.gather(
        plugin.create(uri, runtime_env, context, logger),
        plugin.create(uri, runtime_env, context, logger),
    )

    assert prepare_calls == 1
    assert plugin._manifest_is_valid(plugin._get_cache_path(uri), uri)
    plugin.modify_context([uri], runtime_env, context, logger)
    cache_path = plugin._get_cache_path(uri)
    # With pip, workers are pinned to the exact image the cached environment
    # was built against.
    assert context.container["image"] == "sha256:image"
    assert context.container["entrypoint"] == os.path.join(
        cache_path, "virtualenv", "bin", "python"
    )
    assert context.container["mounts"] == [
        {
            "source": cache_path,
            "target": cache_path,
            "read_only": True,
            "options": "z",
        }
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cache_cleans_failed_staging(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://failed-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {}

    async def fail_prepare(*args, **kwargs):
        raise RuntimeError("installation failed")

    monkeypatch.setattr(plugin, "_prepare_pip_environment", fail_prepare)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    with pytest.raises(RuntimeError, match="installation failed"):
        await plugin.create(uri, runtime_env, RuntimeEnvContext(), logger)

    assert not os.path.exists(plugin._get_cache_path(uri))
    assert not list((tmp_path / "resources" / "image_uri").glob("*.staging-*"))


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cache_cleans_cancelled_staging(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://cancelled-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {}
    prepare_started = asyncio.Event()

    async def block_prepare(*args, **kwargs):
        prepare_started.set()
        await asyncio.Future()

    monkeypatch.setattr(plugin, "_prepare_pip_environment", block_prepare)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    create_task = asyncio.create_task(
        plugin.create(uri, runtime_env, RuntimeEnvContext(), logger)
    )
    await prepare_started.wait()
    assert not os.path.exists(plugin._get_cache_path(uri))

    create_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await create_task

    assert not os.path.exists(plugin._get_cache_path(uri))
    assert not list((tmp_path / "resources" / "image_uri").glob("*.staging-*"))


def test_canonical_pip_config_rejects_virtualenv_name():
    runtime_env = RuntimeEnv(_validate=False)
    dict.__setitem__(runtime_env, "image_uri", "example/image")
    dict.__setitem__(runtime_env, "pip", "preinstalled-venv")
    with pytest.raises(ValueError, match="virtualenv name is not supported"):
        _canonical_pip_config(runtime_env)


def test_image_uri_host_compatibility_warns_without_pip_and_raises_with_pip():
    metadata = replace(
        _image_metadata_for_test(), python_version="0.0.0", ray_version="0.0.0"
    )
    _check_host_compatibility(metadata, "example/image", False, logger)
    with pytest.raises(ValueError, match="does not match the host"):
        _check_host_compatibility(metadata, "example/image", True, logger)


@pytest.mark.asyncio
async def test_image_uri_probe_memoized_by_image_id(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    metadata = replace(
        _image_metadata_for_test(),
        python_version=platform.python_version(),
        ray_version=ray.__version__,
    )
    probe_calls = 0

    async def inspect(image_uri, logger):
        return "example/image", {"Id": metadata.image_id}

    async def probe(image_uri, inspect_data, logger):
        nonlocal probe_calls
        probe_calls += 1
        return metadata

    monkeypatch.setattr("ray._private.runtime_env.image_uri._inspect_image", inspect)
    monkeypatch.setattr("ray._private.runtime_env.image_uri._probe_image", probe)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    first = await plugin.resolve_uris(runtime_env, logger)
    second = await plugin.resolve_uris(runtime_env, logger)

    assert first == second
    assert probe_calls == 1


def test_image_uri_without_pip_uses_image_reference(tmp_path):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://pin-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    context = RuntimeEnvContext()

    plugin.modify_context(
        [uri], RuntimeEnv(image_uri="example/image:tag"), context, logger
    )

    # Without a cached pip environment, workers keep the reference so podman
    # can re-pull a pruned image.
    assert context.container["image"] == "example/image:tag"


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
def test_image_uri_delete_uri_respects_file_lock(tmp_path):
    import fcntl

    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://delete-key"
    cache_path = plugin._get_cache_path(uri)
    os.makedirs(cache_path)

    # Deletion is skipped while another holder keeps the flock.
    with open(cache_path + ".lock", "a+") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert plugin.delete_uri(uri, logger) == 0
        assert os.path.exists(cache_path)
        fcntl.flock(holder, fcntl.LOCK_UN)

    # Unlocked, deletion proceeds; the lock file stays so holders of the old
    # inode never coexist with holders of a recreated one.
    plugin.delete_uri(uri, logger)
    assert not os.path.exists(cache_path)
    assert os.path.exists(cache_path + ".lock")


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
def test_image_uri_set_resources_dir_cleans_only_unlocked_staging(tmp_path):
    import fcntl

    cache_dir = tmp_path / "resources" / "image_uri"
    stale = cache_dir / "stale-key.staging-old"
    stale.mkdir(parents=True)
    live = cache_dir / "live-key.staging-current"
    live.mkdir(parents=True)

    with open(cache_dir / "live-key.lock", "a+") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plugin = ImageURIPlugin(str(tmp_path / "ray"))
        plugin.set_resources_dir(str(tmp_path / "resources"))
        fcntl.flock(holder, fcntl.LOCK_UN)

    assert not stale.exists()
    assert live.exists()


def search_agent(processes):
    for p in processes:
        try:
            for c in p.cmdline():
                # in case linux truncates the proctitle
                if ray_constants.AGENT_PROCESS_TYPE_RUNTIME_ENV_AGENT[:15] in c:
                    return p
        except Exception:
            pass


def check_agent_register(raylet_proc, agent_pid):
    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_raylet_and_agent_share_fate(shutdown_only):
    """Test raylet and agent share fate."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.terminate()
    raylet_proc.wait()
    agent_proc.wait(15)

    # No error should be reported for graceful termination.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 0, errors

    ray.shutdown()

    ray_context = ray.init()
    all_processes = ray._private.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)
    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The raylet should be dead if agent exits.
    agent_proc.kill()
    agent_proc.wait()
    raylet_proc.wait(15)

    worker_node_id = ray_context.address_info["node_id"]
    worker_node_info = [
        node for node in ray.nodes() if node["NodeID"] == worker_node_id
    ][0]
    assert not worker_node_info["Alive"]
    assert worker_node_info["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        "failed and raylet fate-shares with it."
        in worker_node_info["DeathReasonMessage"]
    )


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_agent_report_unexpected_raylet_death(shutdown_only):
    """Test agent reports Raylet death if it is not SIGTERM."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]
    assert (
        os.path.getsize(os.path.join(node.get_session_dir_path(), "logs", "raylet.out"))
        < 1 * 1024**2
    )


@pytest.mark.skipif(sys.platform == "win32", reason="no fate sharing for windows")
def test_agent_report_unexpected_raylet_death_large_file(shutdown_only):
    """Test agent reports Raylet death if it is not SIGTERM."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # Append to the Raylet log file with data >> 1 MB.
    with open(
        os.path.join(node.get_session_dir_path(), "logs", "raylet.out"), "a"
    ) as f:
        f.write("test data\n" * 1024**2)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    # Reading and publishing logs should still work.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
