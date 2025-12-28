# Copyright 2025 Ray authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for `ray kill-actor` CLI."""

import pytest

import ray
from ray.scripts.scripts import cli
from click.testing import CliRunner


def test_kill_actor_by_name_force(ray_start_cluster):
    """Test force-killing a named actor via CLI.

    Covers both regular and detached actors.
    """
    cluster = ray_start_cluster
    address = cluster.address

    runner = CliRunner()

    @ray.remote
    class Actor:
        def ping(self):
            return "pong"

    # Regular named actor
    actor = Actor.options(name="test_actor", namespace="ns").remote()
    assert ray.get(actor.ping.remote()) == "pong"

    result = runner.invoke(
        cli,
        [
            "kill-actor",
            "--address", address,
            "--name", "test_actor",
            "--namespace", "ns",
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output

    with pytest.raises(ray.exceptions.RayActorError, match="might be dead"):
        ray.get(actor.ping.remote())

    # Detached named actor
    detached_actor = Actor.options(
        name="detached_test_actor",
        namespace="ns",
        lifetime="detached",
    ).remote()
    assert ray.get(detached_actor.ping.remote()) == "pong"

    result = runner.invoke(
        cli,
        [
            "kill-actor",
            "--address", address,
            "--name", "detached_test_actor",
            "--namespace", "ns",
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output

    with pytest.raises(ray.exceptions.RayActorError, match="might be dead"):
        ray.get(detached_actor.ping.remote())


def test_kill_actor_by_name_graceful(ray_start_cluster):
    """Test graceful termination of an actor implementing `__ray_terminate__`."""
    cluster = ray_start_cluster
    address = cluster.address

    runner = CliRunner()

    @ray.remote
    class GracefulActor:
        def ping(self):
            return "pong"

        def __ray_terminate__(self):
            import ray.actor
            ray.actor.exit_actor()

    # Regular actor
    actor = GracefulActor.options(name="graceful_actor", namespace="ns").remote()
    assert ray.get(actor.ping.remote()) == "pong"

    result = runner.invoke(
        cli,
        [
            "kill-actor",
            "--address", address,
            "--name", "graceful_actor",
            "--namespace", "ns",
        ],
    )
    assert result.exit_code == 0, result.output

    with pytest.raises(ray.exceptions.RayActorError, match="might be dead"):
        ray.get(actor.ping.remote())

    # Detached actor
    detached_actor = GracefulActor.options(
        name="detached_graceful_actor",
        namespace="ns",
        lifetime="detached",
    ).remote()
    assert ray.get(detached_actor.ping.remote()) == "pong"

    result = runner.invoke(
        cli,
        [
            "kill-actor",
            "--address", address,
            "--name", "detached_graceful_actor",
            "--namespace", "ns",
        ],
    )
    assert result.exit_code == 0, result.output

    with pytest.raises(ray.exceptions.RayActorError, match="might be dead"):
        ray.get(detached_actor.ping.remote())