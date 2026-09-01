import asyncio
import json
import sys
import zipfile
from pathlib import Path
from typing import Dict

import pytest

import ray
from ray._private.runtime_env.archives import ArchivesPlugin
from ray._private.runtime_env.constants import (
    RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import create_for_plugin_if_needed
from ray._private.runtime_env.uri_cache import URICache
from ray.runtime_env import RuntimeEnv, get_archive_paths


def _create_zip(path: Path, files: Dict[str, str]) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return path.as_uri()


def test_get_archive_paths(monkeypatch):
    monkeypatch.delenv(RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match="No archives are available"):
        get_archive_paths()

    monkeypatch.setenv(
        RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR, json.dumps("/tmp/archive")
    )
    assert get_archive_paths() == "/tmp/archive"

    expected = {"model": "/tmp/model", "config": "/tmp/config"}
    monkeypatch.setenv(RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR, json.dumps(expected))
    assert get_archive_paths() == expected

    monkeypatch.setenv(RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR, "not-json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        get_archive_paths()


@pytest.mark.asyncio
async def test_archives_plugin_lifecycle(tmp_path):
    archive_uri = _create_zip(
        tmp_path / "resources.zip",
        {"resource.txt": "resource-data"},
    )
    plugin = ArchivesPlugin(str(tmp_path / "runtime_resources"))
    runtime_env = RuntimeEnv(
        archives={"primary": archive_uri, "duplicate": archive_uri}
    )
    context = RuntimeEnvContext()

    assert plugin.get_uris(runtime_env) == [archive_uri]
    size_bytes = await plugin.create(archive_uri, runtime_env, context)
    assert size_bytes > 0

    plugin.modify_context(plugin.get_uris(runtime_env), runtime_env, context)
    local_paths = json.loads(context.env_vars[RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR])
    assert local_paths["primary"] == local_paths["duplicate"]
    local_dir = Path(local_paths["primary"])
    assert (local_dir / "resource.txt").read_text() == "resource-data"

    uri_cache = URICache(plugin.delete_uri, max_total_size_bytes=0)
    uri_cache.add(archive_uri, size_bytes)
    uri_cache.mark_unused(archive_uri)
    assert uri_cache.get_total_size_bytes() == 0
    assert not local_dir.exists()
    assert plugin.delete_uri(archive_uri) == 0


@pytest.mark.asyncio
async def test_archives_concurrent_setup_is_single_flight(tmp_path, monkeypatch):
    archive_uri = _create_zip(
        tmp_path / "resources.zip",
        {"resource.txt": "resource-data"},
    )
    plugin = ArchivesPlugin(str(tmp_path / "runtime_resources"))
    uri_cache = URICache(plugin.delete_uri)
    contexts = [RuntimeEnvContext(), RuntimeEnvContext()]
    created_sizes = []
    original_create = plugin.create

    async def counted_create(*args, **kwargs):
        size_bytes = await original_create(*args, **kwargs)
        created_sizes.append(size_bytes)
        return size_bytes

    monkeypatch.setattr(plugin, "create", counted_create)
    await asyncio.gather(
        create_for_plugin_if_needed(
            RuntimeEnv(archives=archive_uri), plugin, uri_cache, contexts[0]
        ),
        create_for_plugin_if_needed(
            RuntimeEnv(archives={"resource": archive_uri}),
            plugin,
            uri_cache,
            contexts[1],
        ),
    )

    assert len(created_sizes) == 1
    assert uri_cache.get_total_size_bytes() == created_sizes[0]
    assert all(
        RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR in context.env_vars
        for context in contexts
    )


def test_archives_runtime_env(tmp_path):
    resources_uri = _create_zip(
        tmp_path / "resources.zip",
        {"resource.txt": "resource-data"},
    )
    config_uri = _create_zip(
        tmp_path / "config.zip",
        {"config.json": '{"enabled": true}'},
    )

    try:
        ray.init(num_cpus=1, include_dashboard=False)

        @ray.remote
        def read_single_archive():
            archive_path = Path(get_archive_paths())
            return (archive_path / "resource.txt").read_text()

        @ray.remote
        class NamedArchiveReader:
            def read(self):
                archive_paths = get_archive_paths()
                return {
                    "resource": Path(
                        archive_paths["resource"], "resource.txt"
                    ).read_text(),
                    "duplicate_path": archive_paths["resource"]
                    == archive_paths["duplicate"],
                    "config": Path(archive_paths["config"], "config.json").read_text(),
                }

        assert (
            ray.get(
                read_single_archive.options(
                    runtime_env={"archives": resources_uri}
                ).remote()
            )
            == "resource-data"
        )

        reader = NamedArchiveReader.options(
            runtime_env={
                "archives": {
                    "resource": resources_uri,
                    "duplicate": resources_uri,
                    "config": config_uri,
                }
            }
        ).remote()
        assert ray.get(reader.read.remote()) == {
            "resource": "resource-data",
            "duplicate_path": True,
            "config": '{"enabled": true}',
        }
    finally:
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
