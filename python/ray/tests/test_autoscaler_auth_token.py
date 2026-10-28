import copy
import os
import re
import sys
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple
from unittest.mock import patch

import pytest
import yaml

from ray._private.authentication_test_utils import reset_auth_token_state
from ray.autoscaler._private import commands
from ray.autoscaler._private.fake_multi_node.command_runner import (
    FakeDockerCommandRunner,
)
from ray.autoscaler._private.fake_multi_node.node_provider import (
    FakeMultiNodeDockerProvider,
)
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.exceptions import AuthenticationError
from ray.tests.autoscaler_test_utils import MockProcessRunner, MockProvider

REMOTE_TOKEN_PATH = "~/.ray/auth_token"

CLUSTER_CONFIG = {
    "cluster_name": "default",
    "max_workers": 2,
    "provider": {"type": "mock"},
    "auth": {"ssh_user": "ubuntu", "ssh_private_key": os.devnull},
    "available_node_types": {
        "head": {"node_config": {}, "resources": {"CPU": 1}, "max_workers": 0},
        "worker": {"node_config": {}, "resources": {"CPU": 1}, "max_workers": 2},
    },
    "head_node_type": "head",
    "file_mounts": {},
    "cluster_synced_files": [],
    "initialization_commands": [],
    "setup_commands": [],
    "head_setup_commands": [],
    "worker_setup_commands": [],
    "head_start_ray_commands": ["start_ray_head"],
    "worker_start_ray_commands": [],
}


@pytest.fixture
def clean_auth_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """An empty HOME and no token sources, with this process in token mode."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    for var in ["RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH"]:
        monkeypatch.delenv(var, raising=False)
    set_local_auth_mode(monkeypatch, "token")
    yield tmp_path
    # Restore the caller's RAY_AUTH_MODE before refreshing the cached config.
    monkeypatch.undo()
    reset_auth_token_state()


def set_local_auth_mode(monkeypatch: pytest.MonkeyPatch, mode: Optional[str]) -> None:
    """Set RAY_AUTH_MODE in this process, or unset it for None, and refresh the
    cached RayConfig that get_authentication_mode reads."""
    if mode is None:
        monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    else:
        monkeypatch.setenv("RAY_AUTH_MODE", mode)
    reset_auth_token_state()


class RayUpResult:
    def __init__(
        self,
        runner: MockProcessRunner,
        remote_config: Optional[dict],
        token_uploads: List[Tuple[str, str, int]],
    ):
        self.runner = runner
        self.remote_config = remote_config
        # (source path, content, mode) of each upload to ~/.ray/auth_token,
        # captured while the source still exists.
        self.token_uploads = token_uploads


def ray_up(
    config: Optional[dict] = None,
    no_monitor_on_head: bool = False,
    no_restart: bool = False,
    provider: Optional[MockProvider] = None,
    script_runner: Optional[Callable[[MockProcessRunner], None]] = None,
) -> RayUpResult:
    config = copy.deepcopy(config or CLUSTER_CONFIG)
    provider = provider or MockProvider()
    remote_configs = []

    def capture_remote_config(cluster_config):
        remote_configs.append(copy.deepcopy(cluster_config))
        return cluster_config

    provider.prepare_for_head_node = capture_remote_config

    token_uploads = []
    runner = MockProcessRunner()

    def capture_token_upload():
        cmd = runner.calls[-1]
        if cmd[0] != "rsync" or not cmd[-1].endswith(REMOTE_TOKEN_PATH):
            return
        source = cmd[-2]
        assert "/" in source, cmd
        with open(source) as f:
            content = f.read()
        token_uploads.append((source, content, os.stat(source).st_mode & 0o777))

    runner.cmd_to_callback = {REMOTE_TOKEN_PATH: capture_token_upload}
    if script_runner:
        script_runner(runner)

    commands.get_or_create_head_node(
        config,
        printable_config_file="cluster.yaml",
        no_restart=no_restart,
        restart_only=False,
        yes=True,
        override_cluster_name=None,
        no_monitor_on_head=no_monitor_on_head,
        _provider=provider,
        _runner=runner,
    )
    return RayUpResult(
        runner, remote_configs[0] if remote_configs else None, token_uploads
    )


def test_generates_default_token_and_ships_it_to_head_and_workers(clean_auth_env):
    result = ray_up()

    default_token_path = clean_auth_env / "home" / ".ray" / "auth_token"
    assert default_token_path.read_text()
    # Ray writes no copy of a token it can read from a file.
    assert result.token_uploads == [
        (str(default_token_path), default_token_path.read_text(), 0o600)
    ]
    assert result.remote_config["file_mounts"][REMOTE_TOKEN_PATH] == REMOTE_TOKEN_PATH


def test_ships_env_var_token_through_owner_only_temp_file(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")

    result = ray_up()

    [(source, content, mode)] = result.token_uploads
    assert (content, mode) == ("env-token", 0o600)
    assert not os.path.exists(source)
    assert not any("env-token" in str(cmd) for cmd in result.runner.calls)
    assert not (clean_auth_env / "home" / ".ray" / "auth_token").exists()
    assert result.remote_config["file_mounts"][REMOTE_TOKEN_PATH] == REMOTE_TOKEN_PATH


def test_ships_token_from_token_path(clean_auth_env, monkeypatch):
    token_path = clean_auth_env / "custom_token"
    token_path.write_text("path-token")
    token_path.chmod(0o600)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_path))

    result = ray_up()

    assert result.token_uploads == [(str(token_path), "path-token", 0o600)]
    assert not (clean_auth_env / "home" / ".ray" / "auth_token").exists()


# None (RAY_AUTH_MODE unset) means disabled until AUTH_MODE defaults to token.
@pytest.mark.parametrize("auth_mode", ["disabled", "DISABLED", "none", None])
def test_disabled_auth_mode_ships_no_token(clean_auth_env, monkeypatch, auth_mode):
    set_local_auth_mode(monkeypatch, auth_mode)

    result = ray_up()

    assert result.token_uploads == []
    assert REMOTE_TOKEN_PATH not in result.remote_config["file_mounts"]
    assert not (clean_auth_env / "home" / ".ray" / "auth_token").exists()


def test_keeps_token_mount_from_cluster_yaml(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")
    user_token_path = clean_auth_env / "user_token"
    user_token_path.write_text("yaml-token")
    user_token_path.chmod(0o600)
    config = copy.deepcopy(CLUSTER_CONFIG)
    config["file_mounts"] = {REMOTE_TOKEN_PATH: str(user_token_path)}

    result = ray_up(config)

    assert result.token_uploads == [(str(user_token_path), "yaml-token", 0o600)]


def test_ships_token_to_head_without_monitor(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")

    result = ray_up(no_monitor_on_head=True)

    [(_, content, mode)] = result.token_uploads
    assert (content, mode) == ("env-token", 0o600)


def make_fake_docker_provider(volume_dir: Path) -> FakeMultiNodeDockerProvider:
    return FakeMultiNodeDockerProvider(
        {
            "project_name": "fake_docker",
            "image": "rayproject/ray:nightly",
            "head_resources": {"CPU": 1},
            "shared_volume_dir": str(volume_dir),
        },
        "fake_docker",
    )


def fake_docker_node_volumes(volume_dir: Path) -> Dict[str, List[str]]:
    """Launches a fake docker head, as `ray up` does, and one worker, and
    returns each node's volumes."""
    provider = make_fake_docker_provider(volume_dir)
    for kind in [NODE_KIND_HEAD, NODE_KIND_WORKER]:
        provider.create_node_with_resources_and_labels(
            {},
            {TAG_RAY_NODE_KIND: kind, TAG_RAY_USER_NODE_TYPE: kind},
            1,
            {"CPU": 1},
            {},
        )
    with open(volume_dir / "docker-compose.yaml") as f:
        services = yaml.safe_load(f)["services"]
    return {node_id: spec["volumes"] for node_id, spec in services.items()}


def test_fake_docker_cluster_mounts_token_read_only(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()

    node_volumes = fake_docker_node_volumes(volume_dir)

    token_file = volume_dir / "auth_token"
    assert token_file.read_text() == "env-token"
    assert token_file.stat().st_mode & 0o777 == 0o600
    assert len(node_volumes) == 2
    for volumes in node_volumes.values():
        assert f"{token_file}:/home/ray/.ray/auth_token:ro" in volumes


def test_fake_docker_cluster_disabled_auth_mounts_no_token(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    set_local_auth_mode(monkeypatch, "disabled")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()

    node_volumes = fake_docker_node_volumes(volume_dir)

    assert not (volume_dir / "auth_token").exists()
    for volumes in node_volumes.values():
        assert not any(v.endswith(":/home/ray/.ray/auth_token:ro") for v in volumes)


def test_replaces_empty_default_token_before_shipping(clean_auth_env):
    default_token_path = clean_auth_env / "home" / ".ray" / "auth_token"
    default_token_path.parent.mkdir(parents=True)
    default_token_path.write_text("")

    result = ray_up()

    [(_, content, mode)] = result.token_uploads
    assert content and content == default_token_path.read_text()
    assert mode == 0o600


def test_ships_existing_default_token_unchanged(clean_auth_env):
    default_token_path = clean_auth_env / "home" / ".ray" / "auth_token"
    default_token_path.parent.mkdir(parents=True)
    default_token_path.write_text("existing-token")

    result = ray_up()

    [(_, content, _)] = result.token_uploads
    assert content == "existing-token"
    assert default_token_path.read_text() == "existing-token"


def test_rejects_whitespace_only_env_token(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "   ")

    with pytest.raises(AuthenticationError, match="RAY_AUTH_TOKEN") as error:
        ray_up()
    assert "ray get-auth-token" in str(error.value)
    assert "RAY_AUTH_MODE=disabled" in str(error.value)
    assert ".." not in str(error.value)


def test_missing_token_path_error_names_the_fix(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(clean_auth_env / "missing"))

    with pytest.raises(AuthenticationError, match="RAY_AUTH_TOKEN_PATH") as error:
        ray_up()
    assert "ray get-auth-token" in str(error.value)
    assert "RAY_AUTH_MODE=disabled" in str(error.value)


def test_bad_token_source_launches_no_head(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "   ")
    provider = MockProvider()

    with pytest.raises(AuthenticationError):
        ray_up(provider=provider)
    assert provider.non_terminated_nodes({}) == []


def test_ray_up_and_fake_provider_agree_on_unexpanded_token_path(
    clean_auth_env, monkeypatch
):
    # Ray's token loader opens RAY_AUTH_TOKEN_PATH as given, without
    # expanding "~", so neither launcher may ship a token from it.
    (clean_auth_env / "home").mkdir()
    (clean_auth_env / "home" / "tok").write_text("tilde-token")
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", "~/tok")
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()

    with pytest.raises(AuthenticationError):
        ray_up()
    with pytest.raises(AuthenticationError):
        fake_docker_node_volumes(volume_dir)
    assert not (volume_dir / "auth_token").exists()


def test_reup_of_cluster_without_token_mount_ships_token(clean_auth_env, monkeypatch):
    provider = MockProvider()
    set_local_auth_mode(monkeypatch, "disabled")
    ray_up(provider=provider)
    set_local_auth_mode(monkeypatch, "token")

    result = ray_up(provider=provider)

    [(_, content, _)] = result.token_uploads
    assert content == (clean_auth_env / "home" / ".ray" / "auth_token").read_text()


def test_reup_with_same_token_skips_file_sync(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")
    provider = MockProvider()
    ray_up(provider=provider)

    result = ray_up(provider=provider)

    assert result.token_uploads == []


def test_reup_with_different_token_ships_it(clean_auth_env, monkeypatch):
    provider = MockProvider()
    monkeypatch.setenv("RAY_AUTH_TOKEN", "first-token")
    ray_up(provider=provider)
    monkeypatch.setenv("RAY_AUTH_TOKEN", "second-token")

    result = ray_up(provider=provider)

    assert [content for _, content, _ in result.token_uploads] == ["second-token"]


def test_no_restart_reup_keeps_cluster_token(clean_auth_env, monkeypatch):
    provider = MockProvider()
    monkeypatch.setenv("RAY_AUTH_TOKEN", "first-token")
    ray_up(provider=provider)
    monkeypatch.setenv("RAY_AUTH_TOKEN", "second-token")

    result = ray_up(provider=provider, no_restart=True)

    assert result.token_uploads == []
    # Workers the autoscaler launches later still get the head's own token.
    assert result.remote_config["file_mounts"][REMOTE_TOKEN_PATH] == REMOTE_TOKEN_PATH


def test_plain_reup_after_no_restart_ships_changed_token(clean_auth_env, monkeypatch):
    provider = MockProvider()
    monkeypatch.setenv("RAY_AUTH_TOKEN", "first-token")
    ray_up(provider=provider)
    monkeypatch.setenv("RAY_AUTH_TOKEN", "second-token")
    ray_up(provider=provider, no_restart=True)

    result = ray_up(provider=provider)

    assert [content for _, content, _ in result.token_uploads] == ["second-token"]


DOCKER_CONFIG = {
    **CLUSTER_CONFIG,
    "docker": {"image": "example", "container_name": "mock"},
}


def script_running_container_without_token_mount(runner: MockProcessRunner):
    runner.respond_to_call("json .Mounts", ["[]"])
    runner.respond_to_call("json .Config.Env", ["[]"])
    runner.respond_to_call(".State.Running", ["true"] * 10)


def test_docker_mode_mounts_token_and_restarts_container(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")

    result = ray_up(
        DOCKER_CONFIG, script_runner=script_running_container_without_token_mount
    )

    [(_, content, mode)] = result.token_uploads
    assert (content, mode) == ("env-token", 0o600)
    commands_run = [" ".join(map(str, c)) for c in result.runner.calls]
    assert any(
        re.search(r"rsync .*/tmp/ray_tmp_mount/default/~/\.ray/auth_token$", c)
        for c in commands_run
    )
    result.runner.assert_has_call("1.2.3.4", pattern="docker stop")
    result.runner.assert_has_call(
        "1.2.3.4",
        pattern=r"docker run.*-v /tmp/ray_tmp_mount/default/~/\.ray/auth_token:"
        r"/root/\.ray/auth_token",
    )


def test_fake_docker_runner_skips_only_token_upload(clean_auth_env):
    runner = FakeDockerCommandRunner({"container_name": "mock"})
    with patch.object(runner, "_run_shell") as run_shell, patch.object(
        runner, "run"
    ), patch.object(runner, "_docker_expand_user", side_effect=lambda p: p):
        runner.run_rsync_up("/local/auth_token", REMOTE_TOKEN_PATH)
        assert run_shell.call_count == 0
        runner.run_rsync_up("/local/file", "~/file")
        run_shell.assert_called_once_with("docker cp /local/file mock:~")


def test_fake_docker_provider_removes_partial_token_file(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()

    with patch(
        "ray.autoscaler._private.fake_multi_node.node_provider.os.write",
        side_effect=OSError("disk full"),
    ):
        with pytest.raises(OSError):
            fake_docker_node_volumes(volume_dir)

    assert not (volume_dir / "auth_token").exists()


def test_fake_docker_provider_writes_token_only_once(clean_auth_env, monkeypatch):
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    monkeypatch.setenv("RAY_AUTH_TOKEN", "env-token")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()
    fake_docker_node_volumes(volume_dir)
    monkeypatch.delenv("RAY_AUTH_TOKEN")

    # The head's autoscaler launches later workers without a local token.
    fake_docker_node_volumes(volume_dir)

    assert (volume_dir / "auth_token").read_text() == "env-token"
    assert not (clean_auth_env / "home" / ".ray" / "auth_token").exists()


def test_ships_symlinked_token_file_contents(clean_auth_env, monkeypatch):
    # rsync copies a symlink as a symlink, which would arrive dangling.
    token_path = clean_auth_env / "real_token"
    token_path.write_text("linked-token")
    token_path.chmod(0o600)
    link_path = clean_auth_env / "token_link"
    link_path.symlink_to(token_path)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(link_path))

    result = ray_up()

    assert result.token_uploads == [(str(token_path), "linked-token", 0o600)]


def test_fake_docker_provider_never_creates_a_token(clean_auth_env, monkeypatch):
    # Only `ray up` may create a token; `ray down` builds the provider without one.
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()

    make_fake_docker_provider(volume_dir)
    with pytest.raises(AuthenticationError):
        fake_docker_node_volumes(volume_dir)

    assert not (clean_auth_env / "home" / ".ray" / "auth_token").exists()
    assert not (volume_dir / "auth_token").exists()


def test_fake_docker_provider_mounts_no_token_before_writing_it(
    clean_auth_env, monkeypatch
):
    # The docker monitor starts the head from the compose file the provider
    # writes when built; a bind mount of a missing file would become a directory.
    monkeypatch.setenv("RAY_FAKE_CLUSTER", "1")
    volume_dir = clean_auth_env / "fake_docker"
    volume_dir.mkdir()
    make_fake_docker_provider(volume_dir)

    with open(volume_dir / "docker-compose.yaml") as f:
        [head_spec] = yaml.safe_load(f)["services"].values()
    assert not any(
        v.endswith(":/home/ray/.ray/auth_token:ro") for v in head_spec["volumes"]
    )


def test_creates_token_before_launching_head(clean_auth_env):
    # The fake Docker provider copies the local token when it launches a node.
    default_token_path = clean_auth_env / "home" / ".ray" / "auth_token"
    token_existed_at_launch = []

    class RecordingProvider(MockProvider):
        def create_node(self, node_config, tags, count):
            token_existed_at_launch.append(default_token_path.exists())
            return super().create_node(node_config, tags, count)

    ray_up(provider=RecordingProvider())

    assert token_existed_at_launch == [True]


def test_fake_docker_runner_uploads_token_when_no_volume_holds_it(
    clean_auth_env, monkeypatch
):
    # Outside token mode the provider mounts no token volume, so a token the
    # cluster YAML mounts itself must still be copied in.
    set_local_auth_mode(monkeypatch, "disabled")
    runner = FakeDockerCommandRunner({"container_name": "mock"})
    with patch.object(runner, "_run_shell") as run_shell, patch.object(
        runner, "run"
    ), patch.object(runner, "_docker_expand_user", side_effect=lambda p: p):
        runner.run_rsync_up("/local/auth_token", REMOTE_TOKEN_PATH)
    run_shell.assert_called_once_with("docker cp /local/auth_token mock:~/.ray")


@pytest.mark.parametrize("mode, warns", [(0o644, True), (0o600, False)])
def test_warns_when_token_file_is_readable_by_others(
    clean_auth_env, monkeypatch, mode, warns
):
    # rsync keeps the file's mode, so the head's copy is as readable as this one.
    token_path = clean_auth_env / "token"
    token_path.write_text("path-token")
    token_path.chmod(mode)
    monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token_path))

    with patch.object(commands.cli_logger, "warning") as warning:
        ray_up()

    warned = any(str(token_path) in str(call) for call in warning.call_args_list)
    assert warned == warns


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
