import collections
import json
import numpy as np
import os
import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import signal
import sys
import time

import ray
import ray.ray_constants as ray_constants
from ray.test_utils import (relevant_errors, wait_for_condition,
                            wait_for_errors, wait_for_pid_to_exit,
                            generate_internal_config_map, SignalActor)


@pytest.fixture
def ray_checkpointable_actor_cls(request):
    checkpoint_dir = "/tmp/ray_temp_checkpoint_dir/"
    if not os.path.isdir(checkpoint_dir):
        os.mkdir(checkpoint_dir)

    class CheckpointableActor(ray.actor.Checkpointable):
        def __init__(self):
            self.value = 0
            self.resumed_from_checkpoint = False
            self.checkpoint_dir = checkpoint_dir

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

        def increase(self):
            self.value += 1
            return self.value

        def get(self):
            return self.value

        def was_resumed_from_checkpoint(self):
            return self.resumed_from_checkpoint

        def get_pid(self):
            return os.getpid()

        def should_checkpoint(self, checkpoint_context):
            # Checkpoint the actor when value is increased to 3.
            should_checkpoint = self.value == 3
            return should_checkpoint

        def save_checkpoint(self, actor_id, checkpoint_id):
            actor_id, checkpoint_id = actor_id.hex(), checkpoint_id.hex()
            # Save checkpoint into a file.
            with open(self.checkpoint_dir + actor_id, "a+") as f:
                print(checkpoint_id, self.value, file=f)

        def load_checkpoint(self, actor_id, available_checkpoints):
            actor_id = actor_id.hex()
            filename = self.checkpoint_dir + actor_id
            # Load checkpoint from the file.
            if not os.path.isfile(filename):
                return None

            available_checkpoint_ids = [
                c.checkpoint_id for c in available_checkpoints
            ]
            with open(filename, "r") as f:
                for line in f:
                    checkpoint_id, value = line.strip().split(" ")
                    checkpoint_id = ray.ActorCheckpointID(
                        ray.utils.hex_to_binary(checkpoint_id))
                    if checkpoint_id in available_checkpoint_ids:
                        self.value = int(value)
                        self.resumed_from_checkpoint = True
                        return checkpoint_id
                return None

        def checkpoint_expired(self, actor_id, checkpoint_id):
            pass

    return CheckpointableActor


def test_actor_reconstruction(ray_start_regular):
    """Test actor reconstruction when actor process is killed."""

    @ray.remote(max_reconstructions=1)
    class ReconstructableActor:
        """An actor that will be reconstructed at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, delay=0):
            time.sleep(delay)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = ReconstructableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    result_ids = [actor.increase.remote(delay=0.001) for _ in range(99)]
    # Wait for at least one task to finish and then kill the actor process,
    # while the remaining tasks are still being executed.
    ray.get(result_ids[0])
    os.kill(pid, signal.SIGKILL)
    results = ray.get(result_ids)
    results += [ray.get(actor.increase.remote(delay=0.001))]
    results = np.array(results)
    # Check that the results are of the form [1, 2, ..., N, 1, 2, ..., 100 - N].
    difference_counts = collections.Counter(results[1:] - results[:-1])
    assert difference_counts[1] == 98, difference_counts
    assert collections.Counter(results)[1] == 2

    # Kill actor process one more time.
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    # The actor has exceeded max reconstructions, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())

    # Create another actor.
    actor = ReconstructableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be reconstructed.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())


@pytest.mark.skip("This test does not pass yet.")
def test_actor_reconstruction_without_task(ray_start_regular):
    """Test a dead actor can be reconstructed without a sending task to it."""

    @ray.remote(max_reconstructions=1)
    class ReconstructableActor:
        def __init__(self, signal_actor):
            # Send a signal every time the actor is created.
            signal_actor.send.remote()

        def get_pid(self):
            return os.getpid()

    signal_actor = SignalActor.remote()
    actor = ReconstructableActor.remote(signal_actor)
    # Clear the signal.
    ray.get(signal_actor.wait.remote(clear=True))
    # Kill the actor.
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    # Wait until the actor is reconstructed.
    ray.get(signal_actor.wait.remote(clear=True))


def test_actor_reconstruction_on_node_failure(ray_start_cluster_head):
    """Test actor reconstruction when node dies unexpectedly."""
    cluster = ray_start_cluster_head
    max_reconstructions = 3
    # Add a few nodes to the cluster.
    # Use custom resource to make sure the actor is only created on worker
    # nodes, not on the head node.
    for _ in range(max_reconstructions + 2):
        cluster.add_node(
            resources={"a": 1},
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 200,
                "num_heartbeats_timeout": 10,
            }),
        )

    def kill_node(node_id):
        node_to_remove = None
        for node in cluster.worker_nodes:
            if node_id == node.unique_id:
                node_to_remove = node
        cluster.remove_node(node_to_remove)

    @ray.remote(max_reconstructions=max_reconstructions, resources={"a": 1})
    class MyActor:
        def __init__(self):
            self.value = 0

        def get_value(self):
            return self.value

        def get_object_store_socket(self):
            return ray.worker.global_worker.node.unique_id

    actor = MyActor.remote()

    for i in range(max_reconstructions):
        object_store_socket = ray.get(actor.get_object_store_socket.remote())
        # Kill actor's node and the actor should be reconstructed
        # on a different node.
        kill_node(object_store_socket)
        # Check that the actor is reconstructed.
        assert ray.get(actor.get_value.remote()) == 0
        # Check that the actor is now on a different node.
        assert object_store_socket != ray.get(
            actor.get_object_store_socket.remote())

    # kill the node again.
    object_store_socket = ray.get(actor.get_object_store_socket.remote())
    kill_node(object_store_socket)
    # The actor has exceeded max reconstructions, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.get_value.remote())


def kill_actor(actor):
    """A helper function that kills an actor process."""
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    wait_for_pid_to_exit(pid)


@pytest.mark.skip("This test does not pass yet.")
def test_checkpointing(ray_start_regular, ray_checkpointable_actor_cls):
    """Test actor checkpointing and restoring from a checkpoint."""
    actor = ray.remote(
        max_reconstructions=2)(ray_checkpointable_actor_cls).remote()
    # Call increase 3 times, triggering a checkpoint.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again and check that reconstruction still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


@pytest.mark.skip("This test does not pass yet.")
def test_remote_checkpointing(ray_start_regular, ray_checkpointable_actor_cls):
    """Test checkpointing of a remote actor through method invocation."""

    # Define a class that exposes a method to save checkpoints.
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def __init__(self):
            super(RemoteCheckpointableActor, self).__init__()
            self._should_checkpoint = False

        def checkpoint(self):
            self._should_checkpoint = True

        def should_checkpoint(self, checkpoint_context):
            should_checkpoint = self._should_checkpoint
            self._should_checkpoint = False
            return should_checkpoint

    cls = ray.remote(max_reconstructions=2)(RemoteCheckpointableActor)
    actor = cls.remote()
    # Call increase 3 times.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Call a checkpoint task.
    actor.checkpoint.remote()
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again and check that reconstruction still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


@pytest.mark.skip("This test does not pass yet.")
def test_checkpointing_on_node_failure(ray_start_cluster_2_nodes,
                                       ray_checkpointable_actor_cls):
    """Test actor checkpointing on a remote node."""
    # Place the actor on the remote node.
    cluster = ray_start_cluster_2_nodes
    remote_node = list(cluster.worker_nodes)
    actor_cls = ray.remote(max_reconstructions=1)(ray_checkpointable_actor_cls)
    actor = actor_cls.remote()
    while (ray.get(actor.node_id.remote()) != remote_node[0].unique_id):
        actor = actor_cls.remote()

    # Call increase several times.
    expected = 0
    for _ in range(6):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    cluster.remove_node(remote_node[0])
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


@pytest.mark.skip("This test does not pass yet.")
def test_checkpointing_save_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to complete."""

    @ray.remote(max_reconstructions=2)
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def save_checkpoint(self, actor_id, checkpoint_context):
            raise Exception("Intentional error saving checkpoint.")

    actor = RemoteCheckpointableActor.remote()
    # Call increase 3 times, triggering a checkpoint that will fail.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor still wasn't resumed from a checkpoint and its
    # value is still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again, and check that reconstruction still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that the checkpoint error was pushed to the driver.
    wait_for_errors(ray_constants.CHECKPOINT_PUSH_ERROR, 1)


@pytest.mark.skip("This test does not pass yet.")
def test_checkpointing_load_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to load."""

    @ray.remote(max_reconstructions=2)
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def load_checkpoint(self, actor_id, checkpoints):
            raise Exception("Intentional error loading checkpoint.")

    actor = RemoteCheckpointableActor.remote()
    # Call increase 3 times, triggering a checkpoint that will succeed.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint because loading
    # it failed.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor still wasn't resumed from a checkpoint and its
    # value is still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again, and check that reconstruction still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that the checkpoint error was pushed to the driver.
    wait_for_errors(ray_constants.CHECKPOINT_PUSH_ERROR, 1)


@pytest.mark.skip("This test does not pass yet.")
@pytest.mark.parametrize(
    "ray_start_regular",
    # This overwrite currently isn't effective,
    # see https://github.com/ray-project/ray/issues/3926.
    [generate_internal_config_map(num_actor_checkpoints_to_keep=20)],
    indirect=True,
)
def test_deleting_actor_checkpoint(ray_start_regular):
    """Test deleting old actor checkpoints."""

    @ray.remote
    class CheckpointableActor(ray.actor.Checkpointable):
        def __init__(self):
            self.checkpoint_ids = []

        def get_checkpoint_ids(self):
            return self.checkpoint_ids

        def should_checkpoint(self, checkpoint_context):
            # Save checkpoints after every task
            return True

        def save_checkpoint(self, actor_id, checkpoint_id):
            self.checkpoint_ids.append(checkpoint_id)
            pass

        def load_checkpoint(self, actor_id, available_checkpoints):
            pass

        def checkpoint_expired(self, actor_id, checkpoint_id):
            assert checkpoint_id == self.checkpoint_ids[0]
            del self.checkpoint_ids[0]

    actor = CheckpointableActor.remote()
    for i in range(19):
        assert len(ray.get(actor.get_checkpoint_ids.remote())) == i + 1
    for _ in range(20):
        assert len(ray.get(actor.get_checkpoint_ids.remote())) == 20


@pytest.mark.skip("This test does not pass yet.")
def test_bad_checkpointable_actor_class():
    """Test error raised if an actor class doesn't implement all abstract
    methods in the Checkpointable interface."""

    with pytest.raises(TypeError):

        @ray.remote
        class BadCheckpointableActor(ray.actor.Checkpointable):
            def should_checkpoint(self, checkpoint_context):
                return True


@pytest.mark.skip("This test does not pass yet.")
def test_init_exception_in_checkpointable_actor(ray_start_regular,
                                                ray_checkpointable_actor_cls):
    # This test is similar to test_failure.py::test_failed_actor_init.
    # This test is used to guarantee that checkpointable actor does not
    # break the same logic.
    error_message1 = "actor constructor failed"
    error_message2 = "actor method failed"

    @ray.remote
    class CheckpointableFailedActor(ray_checkpointable_actor_cls):
        def __init__(self):
            raise Exception(error_message1)

        def fail_method(self):
            raise Exception(error_message2)

        def should_checkpoint(self, checkpoint_context):
            return True

    a = CheckpointableFailedActor.remote()

    # Make sure that we get errors from a failed constructor.
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert error_message1 in errors[0]["message"]

    # Make sure that we get errors from a failed method.
    a.fail_method.remote()
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 2
    assert error_message1 in errors[1]["message"]


@pytest.mark.skip("This test does not pass yet.")
def test_decorated_method(ray_start_regular):
    def method_invocation_decorator(f):
        def new_f_invocation(args, kwargs):
            # Split one argument into two. Return th kwargs without passing
            # them into the actor.
            return f([args[0], args[0]], {}), kwargs

        return new_f_invocation

    def method_execution_decorator(f):
        def new_f_execution(self, b, c):
            # Turn two arguments into one.
            return f(self, b + c)

        new_f_execution.__ray_invocation_decorator__ = (
            method_invocation_decorator)
        return new_f_execution

    @ray.remote
    class Actor:
        @method_execution_decorator
        def decorated_method(self, x):
            return x + 1

    a = Actor.remote()

    object_id, extra = a.decorated_method.remote(3, kwarg=3)
    assert isinstance(object_id, ray.ObjectID)
    assert extra == {"kwarg": 3}
    assert ray.get(object_id) == 7  # 2 * 3 + 1


@pytest.mark.skip("This test does not pass yet.")
@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(20)
@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 1,
        "num_nodes": 2,
    }], indirect=True)
def test_ray_wait_dead_actor(ray_start_cluster):
    """Tests that methods completed by dead actors are returned as ready"""
    cluster = ray_start_cluster

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

        def ping(self):
            time.sleep(1)

    # Create some actors and wait for them to initialize.
    num_nodes = len(cluster.list_all_nodes())
    actors = [Actor.remote() for _ in range(num_nodes)]
    ray.get([actor.ping.remote() for actor in actors])

    # Ping the actors and make sure the tasks complete.
    ping_ids = [actor.ping.remote() for actor in actors]
    ray.get(ping_ids)
    # Evict the result from the node that we're about to kill.
    remote_node = cluster.list_all_nodes()[-1]
    remote_ping_id = None
    for i, actor in enumerate(actors):
        if ray.get(actor.node_id.remote()) == remote_node.unique_id:
            remote_ping_id = ping_ids[i]
    ray.internal.free([remote_ping_id], local_only=True)
    cluster.remove_node(remote_node)

    # Repeatedly call ray.wait until the exception for the dead actor is
    # received.
    unready = ping_ids[:]
    while unready:
        _, unready = ray.wait(unready, timeout=0)
        time.sleep(1)

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(ping_ids)

    # Evict the result from the dead node.
    ray.internal.free([remote_ping_id], local_only=True)
    # Create an actor on the local node that will call ray.wait in a loop.
    head_node_resource = "HEAD_NODE"
    ray.experimental.set_resource(head_node_resource, 1)

    @ray.remote(num_cpus=0, resources={head_node_resource: 1})
    class ParentActor:
        def __init__(self, ping_ids):
            self.unready = ping_ids

        def wait(self):
            _, self.unready = ray.wait(self.unready, timeout=0)
            return len(self.unready) == 0

        def ping(self):
            return

    # Repeatedly call ray.wait through the local actor until the exception for
    # the dead actor is received.
    parent_actor = ParentActor.remote(ping_ids)
    ray.get(parent_actor.ping.remote())
    failure_detected = False
    while not failure_detected:
        failure_detected = ray.get(parent_actor.wait.remote())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
