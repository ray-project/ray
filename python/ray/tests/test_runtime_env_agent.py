import asyncio
import json
import logging
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
from dataclasses import replace
from typing import List, Tuple

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private import ray_constants
from ray._private.runtime_env.agent.runtime_env_agent import (
    ReferenceTable,
    SetupLoggerFactory,
    UriType,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import (
    _CACHE_SCHEMA_VERSION,
    _INSTALL_ENV_PATH,
    ImageMetadata,
    ImageURIPlugin,
    _canonical_pip_config,
    _check_host_compatibility,
    _get_image_uri_cache_key,
    _write_install_env,
)
from ray._private.runtime_env.plugin import create_for_plugin_if_needed
from ray._private.runtime_env.uri_cache import URICache
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


def test_reference_table_releases_uris_bound_before_delete(caplog):
    unused_uris = []
    unused_runtime_envs = []
    reference_table = ReferenceTable(
        lambda runtime_env: [],
        unused_uris.extend,
        unused_runtime_envs.append,
    )
    runtime_env = RuntimeEnv(
        image_uri="example/image:latest",
        pip=["package==1"],
        env_vars={"API_KEY": "secret-value"},
    )
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
    with caplog.at_level(logging.INFO):
        reference_table.release_dynamic_uris_if_unreferenced(
            serialized_env, "raylet", [dynamic_uri]
        )
    assert unused_uris == [dynamic_uri]
    assert unused_runtime_envs == [serialized_env]
    assert "lost all references during creation" in caplog.text
    assert "API_KEY" in caplog.text
    assert "secret-value" not in caplog.text


def test_reference_table_rebinds_uris_after_recreate_race():
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

    # The first reference is deleted after URI resolution. A new reference
    # arrives before creation completes, after the old URI binding was popped.
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")
    reference_table.add_dynamic_uris(serialized_env, [dynamic_uri])
    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
    unused_uris.clear()
    unused_runtime_envs.clear()
    reference_table.increase_reference(runtime_env, serialized_env, "raylet")

    reference_table.release_dynamic_uris_if_unreferenced(
        serialized_env, "raylet", [dynamic_uri]
    )

    assert reference_table._dynamic_uris[serialized_env] == [dynamic_uri]
    assert reference_table._uri_reference[dynamic_uri[0]] == 1
    assert unused_uris == []
    assert unused_runtime_envs == []

    reference_table.decrease_reference(runtime_env, serialized_env, "raylet")
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


def test_image_uri_install_env_includes_explicit_env_vars(monkeypatch):
    monkeypatch.setenv("PIP_INDEX_URL", "https://host.example/simple")
    monkeypatch.setenv("UNRELATED_HOST_VAR", "host-only")
    runtime_env = RuntimeEnv(
        image_uri="example/image",
        pip=["package==1"],
        env_vars={
            "BUILD_TOKEN": "secret",
            "CFLAGS": "-DUSE_FEATURE",
            "PIP_INDEX_URL": "https://runtime.example/simple",
        },
    )

    install_env = ImageURIPlugin._get_install_env(runtime_env)

    assert install_env["BUILD_TOKEN"] == "secret"
    assert install_env["CFLAGS"] == "-DUSE_FEATURE"
    assert install_env["PIP_INDEX_URL"] == "https://runtime.example/simple"
    assert "UNRELATED_HOST_VAR" not in install_env


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
        metadata, staging_path, final_path, entrypoint, args, env_file, logger
    ):
        calls.append((entrypoint, args, env_file))
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

    assert calls[0][:2] == (
        metadata.python_executable,
        [
            "-m",
            "venv",
            "--system-site-packages",
            "--without-pip",
            os.path.join(final_path, "virtualenv"),
        ],
    )
    # Nothing to pass, so no env file.
    assert all(env_file is None for _, _, env_file in calls)


@pytest.mark.skipif(sys.platform == "win32", reason="image_uri is Linux-only.")
@pytest.mark.parametrize("outcome", ["success", "failure", "cancelled"])
@pytest.mark.asyncio
async def test_image_uri_install_env_reaches_podman_through_json(
    tmp_path, monkeypatch, outcome
):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    uri = "image-pip://cache-key"
    metadata = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {
        "BUILD_TOKEN": "secret",
        "LD_PRELOAD": "/job.so",
        "PATH": "/image/bin",
        "MULTILINE": "first\nsecond\r\n",
        "LONG": "x" * 70000,
        "QUOTED": "\"'$VALUE`command`中文",
        "EMPTY": "",
        "# NAME\n": "valid OS name",
    }
    calls = []

    async def check_output(command, logger, **kwargs):
        mount = next(
            arg for arg in command if arg.endswith(f":{_INSTALL_ENV_PATH}:ro,z")
        )
        env_file = mount.removesuffix(f":{_INSTALL_ENV_PATH}:ro,z")
        with open(env_file, encoding="utf-8") as file:
            contents = json.load(file)
        env_dir_mode = stat.S_IMODE(os.stat(os.path.dirname(env_file)).st_mode)
        env_file_mode = stat.S_IMODE(os.stat(env_file).st_mode)
        calls.append((command, kwargs, env_file, contents, env_dir_mode, env_file_mode))
        if outcome == "failure":
            raise RuntimeError("installation failed")
        if outcome == "cancelled":
            raise asyncio.CancelledError()
        return f"RAY_VERSION={metadata.ray_version}\n"

    async def cleanup(*args):
        pass

    monkeypatch.setattr(
        "ray._private.runtime_env.image_uri._cleanup_container", cleanup
    )
    monkeypatch.setattr(
        "ray._private.runtime_env.image_uri.check_output_cmd", check_output
    )
    staging_path = str(tmp_path / "staging")
    os.makedirs(staging_path)

    async def prepare():
        await plugin._prepare_pip_environment(
            uri,
            RuntimeEnv(
                image_uri="example/image",
                pip={
                    "packages": ["package==1"],
                    "pip_version": "==25.0",
                    "pip_check": True,
                },
            ),
            metadata,
            staging_path,
            str(tmp_path / "final"),
            logger,
        )

    if outcome == "success":
        await prepare()
        assert len(calls) == 5
    else:
        error = RuntimeError if outcome == "failure" else asyncio.CancelledError
        with pytest.raises(error):
            await prepare()

    assert calls
    for command, kwargs, _, contents, env_dir_mode, env_file_mode in calls:
        # podman runs with the agent's environment, and the logged argv
        # carries no values.
        assert kwargs == {}
        assert not any("secret" in arg or "/job.so" in arg for arg in command)
        assert contents == plugin._install_env_by_uri[uri]
        assert env_dir_mode == 0o700
        assert env_file_mode == 0o600
        assert command[command.index("--entrypoint") + 1] == metadata.python_executable
    # The file only exists while the installation runs.
    assert not os.path.exists(calls[0][2])


@pytest.mark.parametrize(
    "name,value",
    [
        ("NAME", "secret\0"),
        ("NAME=X", "secret"),
        ("NAME\0", "secret"),
        ("", "secret"),
    ],
)
def test_image_uri_install_env_rejects_invalid_os_entries(tmp_path, name, value):
    with pytest.raises(ValueError, match="can't be passed") as error:
        _write_install_env(str(tmp_path / "env"), {name: value})
    # Values can be secret, so the error names only the variable.
    assert "secret" not in str(error.value)


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
        virtualenv_python = os.path.join(args[3], "virtualenv", "bin", "python")
        os.makedirs(os.path.dirname(virtualenv_python))
        os.symlink("/inside-image/bin/python", virtualenv_python)
        await asyncio.sleep(0)

    monkeypatch.setattr(plugin, "_prepare_pip_environment", prepare)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    context = RuntimeEnvContext()
    await asyncio.gather(
        plugin.create(uri, runtime_env, context, logger),
        plugin.create(uri, runtime_env, context, logger),
    )

    assert prepare_calls == 1
    assert plugin._manifest_is_valid(plugin._get_cache_path(uri), uri, has_pip=True)
    plugin.modify_context([uri], runtime_env, context, logger)
    cache_path = plugin._get_cache_path(uri)
    # With pip, workers are pinned to the exact image the cached environment
    # was built against.
    assert context.container["image"] == "sha256:image"
    assert context.container["entrypoint"] == os.path.join(
        cache_path, "virtualenv", "bin", "python"
    )
    command = context.container["command"]
    assert command[command.index(f"{cache_path}:{cache_path}:ro,z") - 1] == "-v"


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cache_shared_between_plugin_instances(tmp_path, monkeypatch):
    plugins = [
        ImageURIPlugin(str(tmp_path / "ray")),
        ImageURIPlugin(str(tmp_path / "ray")),
    ]
    uri = "image-pip://shared-key"
    prepare_calls = 0

    async def prepare(*args, **kwargs):
        nonlocal prepare_calls
        prepare_calls += 1
        virtualenv_python = os.path.join(args[3], "virtualenv", "bin", "python")
        os.makedirs(os.path.dirname(virtualenv_python))
        os.symlink("/inside-image/bin/python", virtualenv_python)
        await asyncio.sleep(0)

    for plugin in plugins:
        plugin.set_resources_dir(str(tmp_path / "resources"))
        plugin._metadata_by_uri[uri] = _image_metadata_for_test()
        plugin._install_env_by_uri[uri] = {}
        monkeypatch.setattr(plugin, "_prepare_pip_environment", prepare)

    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    sizes = await asyncio.gather(
        *[
            plugin.create(uri, runtime_env, RuntimeEnvContext(), logger)
            for plugin in plugins
        ]
    )

    assert prepare_calls == 1
    assert sizes[0] == sizes[1]
    assert plugins[1]._manifest_is_valid(
        plugins[1]._get_cache_path(uri), uri, has_pip=True
    )
    for plugin in plugins:
        assert not os.get_inheritable(plugin._cache_entries[uri].use_lock.fileno())
    plugins[0].on_uri_unused(uri)
    assert plugins[0].delete_uri(uri) is None
    plugins[1].on_uri_unused(uri)
    assert plugins[0].delete_uri(uri) == sizes[0]


def _evict_image_uri_in_subprocess(resources_dir, uri):
    script = """
import json, sys
from ray._private.runtime_env.image_uri import ImageURIPlugin
plugin = ImageURIPlugin('/tmp/ray')
plugin.set_resources_dir(sys.argv[1])
print(json.dumps(plugin.delete_uri(sys.argv[2])))
"""
    return json.loads(
        subprocess.check_output(
            [sys.executable, "-c", script, str(resources_dir), uri],
            text=True,
            timeout=30,
        )
    )


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cache_hit_holds_lease_through_use(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    resources = tmp_path / "resources"
    plugin.set_resources_dir(str(resources))
    uri = "image-pip://leased-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    env = RuntimeEnv(image_uri="example/image")
    await plugin.create(uri, env, RuntimeEnvContext(), logger)
    plugin.on_uri_unused(uri)
    read_size = plugin._read_manifest_size

    def measure(path):
        # This separate process tries eviction after validation but before
        # the hit can read its size or hand its lease back to the cache.
        assert _evict_image_uri_in_subprocess(resources, uri) is None
        return read_size(path)

    monkeypatch.setattr(plugin, "_read_manifest_size", measure)
    await plugin.create(uri, env, RuntimeEnvContext(), logger)
    assert _evict_image_uri_in_subprocess(resources, uri) is None
    assert os.path.exists(plugin._get_cache_path(uri))
    plugin.on_uri_unused(uri)
    assert _evict_image_uri_in_subprocess(resources, uri) == 0
    assert not os.path.exists(plugin._get_cache_path(uri))


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_revalidates_unused_local_cache(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    sibling = ImageURIPlugin(str(tmp_path / "ray"))
    for instance in (plugin, sibling):
        instance.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://recreated-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    cache = URICache(
        plugin.delete_uri, on_uri_unused=plugin.on_uri_unused, debug_mode=True
    )
    env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    installs = 0

    async def prepare(*args):
        nonlocal installs
        installs += 1
        python = os.path.join(args[3], "virtualenv", "bin", "python")
        os.makedirs(os.path.dirname(python))
        with open(python, "w") as file:
            file.write("x" * (10 * installs))
        os.chmod(python, 0o755)

    monkeypatch.setattr(plugin, "_prepare_pip_environment", prepare)
    for expected_installs in (1, 2):
        context = RuntimeEnvContext()
        await create_for_plugin_if_needed(
            env, plugin, cache, context, resolved_uris=[uri]
        )
        assert installs == expected_installs
        assert cache.get_total_size_bytes() == expected_installs * 10
        assert os.path.exists(context.container["entrypoint"])
        assert sibling.delete_uri(uri) is None
        cache.mark_unused(uri)
        assert sibling.delete_uri(uri) == expected_installs * 10
        assert uri in cache  # This agent still has the now-stale local entry.

    cache.max_total_size_bytes = 0
    cache._evict_if_needed()
    assert uri not in cache
    assert cache.get_total_size_bytes() == 0


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.parametrize("recreate", [False, True])
@pytest.mark.asyncio
async def test_image_uri_reference_handoff_during_setup(
    tmp_path, monkeypatch, recreate
):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    sibling = ImageURIPlugin(str(tmp_path / "ray"))
    for instance in (plugin, sibling):
        instance.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://handoff-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    cache = URICache(plugin.delete_uri, on_uri_unused=plugin.on_uri_unused)
    contexts = {}
    refs = ReferenceTable(
        lambda env: [],
        lambda uris: [cache.mark_unused(uri) for uri, _ in uris],
        lambda env: contexts.pop(env, None),
    )
    env = RuntimeEnv(image_uri="example/image")
    serialized = env.serialize()
    dynamic_uris = [(uri, "image_uri")]
    refs.increase_reference(env, serialized, "raylet")
    refs.add_dynamic_uris(serialized, dynamic_uris)
    published, finish = asyncio.Event(), asyncio.Event()
    publish = plugin._publish_environment

    async def pause_publication(*args):
        size = await publish(*args)
        published.set()
        await finish.wait()
        return size

    monkeypatch.setattr(plugin, "_publish_environment", pause_publication)
    setup = asyncio.create_task(
        create_for_plugin_if_needed(
            env, plugin, cache, RuntimeEnvContext(), resolved_uris=[uri]
        )
    )
    await published.wait()
    refs.decrease_reference(env, serialized, "raylet")
    assert uri not in cache  # The unused notification must reach in-flight setup.
    assert sibling.delete_uri(uri) is None
    if recreate:
        refs.increase_reference(env, serialized, "raylet")
    finish.set()
    await setup
    contexts[serialized] = "cached context"
    refs.release_dynamic_uris_if_unreferenced(serialized, "raylet", dynamic_uris)
    if recreate:
        # A cached context hit adds a reference without re-running create.
        refs.increase_reference(env, serialized, "raylet")
        refs.decrease_reference(env, serialized, "raylet")
        assert serialized in contexts
        assert sibling.delete_uri(uri) is None
        refs.decrease_reference(env, serialized, "raylet")
    assert serialized not in contexts
    assert sibling.delete_uri(uri) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_lease_released_on_process_exit(tmp_path):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    resources = tmp_path / "resources"
    plugin.set_resources_dir(str(resources))
    uri = "image-pip://process-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    await plugin.create(
        uri, RuntimeEnv(image_uri="example/image"), RuntimeEnvContext(), logger
    )
    plugin.on_uri_unused(uri)
    script = """
import asyncio, logging, sys
from ray._private.runtime_env.image_uri import ImageURIPlugin
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.runtime_env import RuntimeEnv
plugin = ImageURIPlugin('/tmp/ray')
plugin.set_resources_dir(sys.argv[1])
uri = sys.argv[2]
plugin._metadata_by_uri[uri] = None  # The existing manifest is sufficient on a hit.
asyncio.run(plugin.create(uri, RuntimeEnv(image_uri='example/image'),
                          RuntimeEnvContext(), logging.getLogger()))
print('ready', flush=True)
sys.stdin.read()
"""
    with subprocess.Popen(
        [sys.executable, "-c", script, str(resources), uri],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert process.stdout.readline().strip() == "ready"
            assert plugin.delete_uri(uri) is None
        finally:
            process.kill()
            process.wait(timeout=30)
    assert plugin.delete_uri(uri) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cancelled_setup_keeps_waiter_state(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://waiter-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    env = RuntimeEnv(image_uri="example/image")
    started = asyncio.Event()
    publish = plugin._publish_environment
    attempts = 0

    async def pause_first_attempt(*args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            started.set()
            await asyncio.Event().wait()
        return await publish(*args)

    monkeypatch.setattr(plugin, "_publish_environment", pause_first_attempt)
    first = asyncio.create_task(plugin.create(uri, env, RuntimeEnvContext(), logger))
    await started.wait()
    second = asyncio.create_task(plugin.create(uri, env, RuntimeEnvContext(), logger))
    await asyncio.sleep(0)  # Let the second setup register as a lock waiter.
    entry = plugin._cache_entries[uri]
    assert entry.setups == 2
    plugin.on_uri_unused(uri)
    assert plugin.delete_uri(uri) is None
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert plugin._cache_entries[uri] is entry
    assert entry.use_lock is not None
    assert entry.setups == 0
    plugin.on_uri_unused(uri)
    assert plugin.delete_uri(uri) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="symlink test is POSIX-only.")
def test_image_uri_manifest_requires_complete_pip_environment(tmp_path):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://manifest-key"
    cache_path = plugin._get_cache_path(uri)
    os.makedirs(cache_path)
    manifest_path = os.path.join(cache_path, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as manifest_file:
        json.dump(
            {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "uri": uri,
                "has_pip": True,
                "size_bytes": 1234,
            },
            manifest_file,
        )

    assert not plugin._manifest_is_valid(cache_path, uri, has_pip=True)

    virtualenv_python = os.path.join(cache_path, "virtualenv", "bin", "python")
    os.makedirs(os.path.dirname(virtualenv_python))
    os.symlink("/inside-image/bin/python", virtualenv_python)

    assert not os.path.exists(virtualenv_python)
    assert os.path.lexists(virtualenv_python)
    assert plugin._manifest_is_valid(cache_path, uri, has_pip=True)
    assert not plugin._manifest_is_valid(cache_path, uri, has_pip=False)


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
    assert not [
        path
        for path in (tmp_path / "resources" / "image_uri").glob("*.staging-*")
        if ".deleting-" not in path.name
    ]


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
    assert not [
        path
        for path in (tmp_path / "resources" / "image_uri").glob("*.staging-*")
        if ".deleting-" not in path.name
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
@pytest.mark.asyncio
async def test_image_uri_cancelled_cleanup_cannot_remove_retry(tmp_path, monkeypatch):
    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://cleanup-retry"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {}
    final_path = plugin._get_cache_path(uri)
    os.makedirs(final_path)  # An incomplete entry from an earlier attempt.
    deleting = threading.Event()
    resume_delete = threading.Event()
    deleted = threading.Event()
    real_rmtree = shutil.rmtree
    first_delete = True
    block_prepare = True

    def slow_rmtree(path, *args, **kwargs):
        nonlocal first_delete
        if first_delete:
            first_delete = False
            deleting.set()
            try:
                assert resume_delete.wait(10)
                return real_rmtree(path, *args, **kwargs)
            finally:
                deleted.set()
        return real_rmtree(path, *args, **kwargs)

    async def prepare(uri, env, metadata, staging_path, final_path, logger):
        if block_prepare:
            await asyncio.Future()
        python_path = os.path.join(staging_path, "virtualenv", "bin", "python")
        os.makedirs(os.path.dirname(python_path))
        os.symlink("/inside-image/python", python_path)

    monkeypatch.setattr("ray._private.runtime_env.image_uri.shutil.rmtree", slow_rmtree)
    monkeypatch.setattr(plugin, "_prepare_pip_environment", prepare)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    first = asyncio.create_task(
        plugin.create(uri, runtime_env, RuntimeEnvContext(), logger)
    )
    try:
        assert await asyncio.to_thread(deleting.wait, 10)
        first.cancel()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        block_prepare = False
        await plugin.create(uri, runtime_env, RuntimeEnvContext(), logger)
    finally:
        resume_delete.set()
        assert await asyncio.to_thread(deleted.wait, 10)

    assert plugin._manifest_is_valid(final_path, uri, has_pip=True)


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
        await asyncio.sleep(0)
        return metadata

    monkeypatch.setattr("ray._private.runtime_env.image_uri._inspect_image", inspect)
    monkeypatch.setattr("ray._private.runtime_env.image_uri._probe_image", probe)
    runtime_env = RuntimeEnv(image_uri="example/image", pip=["package==1"])
    first, second = await asyncio.gather(
        plugin.resolve_uris(runtime_env, logger),
        plugin.resolve_uris(runtime_env, logger),
    )

    assert first == second
    assert probe_calls == 1


@pytest.mark.skipif(sys.platform == "win32", reason="fcntl is Linux-only.")
def test_image_uri_delete_uri_respects_file_lock(tmp_path):
    import fcntl

    plugin = ImageURIPlugin(str(tmp_path / "ray"))
    plugin.set_resources_dir(str(tmp_path / "resources"))
    uri = "image-pip://delete-key"
    plugin._metadata_by_uri[uri] = _image_metadata_for_test()
    plugin._install_env_by_uri[uri] = {}
    cache_path = plugin._get_cache_path(uri)
    os.makedirs(cache_path)
    with open(
        os.path.join(cache_path, "manifest.json"), "w", encoding="utf-8"
    ) as manifest_file:
        json.dump(
            {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "uri": uri,
                "has_pip": False,
                "size_bytes": 1234,
            },
            manifest_file,
        )

    # Deletion is deferred while another holder keeps the flock, and the URI
    # remains tracked so a later eviction pass can retry it.
    with open(cache_path + ".lock", "a+") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert plugin.delete_uri(uri, logger) is None
        assert os.path.exists(cache_path)
        assert uri in plugin._metadata_by_uri
        assert uri in plugin._install_env_by_uri
        fcntl.flock(holder, fcntl.LOCK_UN)

    # Unlocked, deletion proceeds; the lock file stays so holders of the old
    # inode never coexist with holders of a recreated one.
    assert plugin.delete_uri(uri, logger) == 1234
    assert not os.path.exists(cache_path)
    assert os.path.exists(cache_path + ".lock")
    assert uri not in plugin._metadata_by_uri
    assert uri not in plugin._install_env_by_uri


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


def _make_setup_logger_factory(log_dir, logging_level=logging.INFO):
    return SetupLoggerFactory(
        logging_level=logging_level,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=str(log_dir),
        max_bytes=1024 * 1024,
        backup_count=1,
    )


def test_setup_logger_releases_file_handles_and_keeps_file(tmp_path):
    """The FD is scoped to the setup; the log file on disk is not."""
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        logger.info("hello")
        assert len(factory._pooled_handlers) == 1
        (entry,) = factory._pooled_handlers.values()
    # Pool is empty and the handler's stream is closed...
    assert factory._pooled_handlers == {}
    assert entry.handler.stream is None
    # ...but the file is still on disk for the log monitor and dashboard.
    log_file = tmp_path / "runtime_env_setup-01000000.log"
    assert "hello" in log_file.read_text()


def test_setup_logger_shares_handler_across_concurrent_setups(tmp_path):
    """Two jobs writing the same log_files entry share one reference-counted
    handler; the file closes only when the last setup exits."""
    factory = _make_setup_logger_factory(tmp_path)
    shared_path = os.path.abspath(str(tmp_path / "shared.log"))
    with factory.setup_logger("0aaa", ["shared.log"]) as first:
        with factory.setup_logger("0bbb", ["shared.log"]) as second:
            entry = factory._pooled_handlers[shared_path]
            assert entry.ref_count == 2
            assert entry.handler in first.handlers
            assert entry.handler in second.handlers
            # Two per-job setup logs plus the one shared handler.
            assert len(factory._pooled_handlers) == 3
            first.info("from first")
            second.info("from second")
        # Inner setup exited: still open for the outer one.
        assert factory._pooled_handlers[shared_path].ref_count == 1
        assert entry.handler.stream is not None
    assert factory._pooled_handlers == {}
    assert entry.handler.stream is None
    content = (tmp_path / "shared.log").read_text()
    assert "from first" in content and "from second" in content


@pytest.mark.parametrize("exit_path", ["exception", "timeout"])
def test_setup_logger_releases_on_error_paths(tmp_path, exit_path):
    factory = _make_setup_logger_factory(tmp_path)
    if exit_path == "exception":
        with pytest.raises(RuntimeError):
            with factory.setup_logger("01000000", []) as logger:
                logger.info("started")
                raise RuntimeError("setup failed")
    else:

        async def _setup():
            with factory.setup_logger("01000000", []) as logger:
                logger.info("started")
                await asyncio.sleep(60)

        async def _run():
            with pytest.raises(asyncio.TimeoutError):
                # wait_for awaits the cancelled coroutine before raising, so
                # the context manager unwinds before the timeout is reported.
                await asyncio.wait_for(_setup(), timeout=0.05)

        asyncio.run(_run())
    assert factory._pooled_handlers == {}


def test_setup_logger_not_in_global_registry(tmp_path):
    """The per-setup logger must not be resurrectable via logging.getLogger."""
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        assert logger.name == "runtime_env_01000000"
        assert logger.name not in logging.Logger.manager.loggerDict
    assert "runtime_env_01000000" not in logging.Logger.manager.loggerDict


def test_setup_logger_late_write_does_not_reopen_file(tmp_path):
    """A write after release must not silently reopen the file.

    FileHandler.emit() lazily reopens its file when the stream is None, so a
    closed handler that is still attached to the logger would leak the
    descriptor again with no owner left to close it. This pins the
    detach-before-close ordering.
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        (entry,) = factory._pooled_handlers.values()
    logger.warning("late write after release")
    assert entry.handler.stream is None
    assert factory._pooled_handlers == {}
    assert "late write" not in (tmp_path / "runtime_env_setup-01000000.log").read_text()
    assert all(isinstance(h, logging.NullHandler) for h in logger.handlers)


def test_setup_logger_concurrent_late_emit_does_not_reopen_file(tmp_path):
    """A racing thread that grabbed the handler before it was detached must
    not reopen the file by emitting after close.

    Logger.callHandlers reads the handler list before Handler.handle runs, so
    an executor thread (e.g. conda work logging after a setup timeout) can
    hold the handler across the detach + close. FileHandler.emit lazily
    reopens the file in append mode, which would leak a descriptor with no
    owner left to close it. Calling handle() on the handler directly
    simulates that thread.
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        (entry,) = factory._pooled_handlers.values()
        handler = entry.handler
        logger.info("in scope")
    record = logging.LogRecord(
        "runtime_env_01000000",
        logging.WARNING,
        __file__,
        0,
        "late concurrent write",
        None,
        None,
    )
    handler.handle(record)
    assert handler.stream is None
    log_text = (tmp_path / "runtime_env_setup-01000000.log").read_text()
    assert "late concurrent write" not in log_text


def test_setup_logger_release_swaps_handler_list_atomically(tmp_path):
    """Release must replace the handler list object, not mutate it in place.

    A concurrent late write iterates whichever list object it read first, so
    the old list must stay intact as a snapshot (its handlers refuse to emit
    once closed) and the new list must never be empty — an empty handler list
    falls through to logging.lastResort on the agent's stderr (the logger has
    propagate off and no parent).
    """
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", []) as logger:
        old_handler_list = logger.handlers
        old_snapshot = list(old_handler_list)
        assert old_snapshot
    assert logger.handlers is not old_handler_list
    assert list(old_handler_list) == old_snapshot
    assert logger.handlers
    assert all(isinstance(h, logging.NullHandler) for h in logger.handlers)


def test_setup_logger_dedupes_paths_within_one_setup(tmp_path):
    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("01000000", ["dup.log", "dup.log", "./dup.log"]):
        entry = factory._pooled_handlers[os.path.abspath(str(tmp_path / "dup.log"))]
        assert entry.ref_count == 1
    assert factory._pooled_handlers == {}


def test_setup_logger_matches_component_logger_format_and_level(tmp_path):
    """Parity with setup_component_logger: level filtering and LOGGER_FORMAT.

    A bare logging.Logger would pass every DEBUG record (NOTSET with no
    parent) and a bare handler would write unformatted messages; nothing else
    in the suite would notice either regression.
    """
    factory = _make_setup_logger_factory(tmp_path, logging_level=logging.INFO)
    with factory.setup_logger("01000000", []) as logger:
        logger.debug("must be filtered")
        logger.info("visible message")
    lines = (tmp_path / "runtime_env_setup-01000000.log").read_text().splitlines()
    assert not any("must be filtered" in line for line in lines)
    (line,) = [line for line in lines if "visible message" in line]
    # LOGGER_FORMAT: "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
    assert re.match(
        r"^\d{4}-\d{2}-\d{2} [\d:,]+\tINFO \S+:\d+ -- visible message$", line
    ), line


def test_setup_logger_accepts_string_logging_level(tmp_path):
    factory = SetupLoggerFactory(
        logging_level="info",
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=str(tmp_path),
        max_bytes=0,
        backup_count=1,
    )
    with factory.setup_logger("01000000", []) as logger:
        assert logger.level == logging.INFO


@pytest.mark.skipif(sys.platform != "linux", reason="/proc/self/fd is Linux-only")
def test_setup_logger_fd_count_stays_flat_across_many_setups(tmp_path):
    """The regression that motivated this: FDs must scale with in-flight
    setups, not with the cumulative number of jobs (#54935)."""

    def fd_count():
        return len(os.listdir("/proc/self/fd"))

    factory = _make_setup_logger_factory(tmp_path)
    with factory.setup_logger("warmup", []) as logger:
        logger.info("warmup")
    before = fd_count()
    for i in range(200):
        with factory.setup_logger(f"{i:08d}", []) as logger:
            logger.info("job %d", i)
    assert fd_count() <= before + 1


def test_setup_logger_falls_back_to_stream_handler_without_log_dir(tmp_path):
    """Mirrors setup_component_logger: no log_dir means stderr, nothing pooled."""
    factory = SetupLoggerFactory(
        logging_level=logging.INFO,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir="",
        max_bytes=0,
        backup_count=1,
    )
    with factory.setup_logger("01000000", []) as logger:
        assert factory._pooled_handlers == {}
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
