import collections
import json
import numpy as np
import os
import pytest
import signal
import sys
import time

import ray
import ray.ray_constants as ray_constants
import ray.test_utils
import ray.cluster_utils
from ray.test_utils import (relevant_errors, wait_for_condition,
                            wait_for_errors, wait_for_pid_to_exit,
                            generate_internal_config_map)


@pytest.fixture
def ray_checkpointable_actor_cls(request):
    checkpoint_dir = os.path.join(ray.utils.get_user_temp_dir(),
                                  "ray_temp_checkpoint_dir") + os.sep
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


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "object_store_memory": 150 * 1024 * 1024,
        "lru_evict": True,
    }],
    indirect=True)
def test_actor_eviction(ray_start_regular):
    object_store_memory = 150 * 1024 * 1024

    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def create_object(self, size):
            return np.random.rand(size)

    a = Actor.remote()
    # Submit enough methods on the actor so that they exceed the size of the
    # object store.
    objects = []
    num_objects = 20
    for _ in range(num_objects):
        obj = a.create_object.remote(object_store_memory // num_objects)
        objects.append(obj)
        # Get each object once to make sure each object gets created.
        ray.get(obj)

    # Get each object again. At this point, the earlier objects should have
    # been evicted.
    num_evicted, num_success = 0, 0
    for obj in objects:
        try:
            val = ray.get(obj)
            assert isinstance(val, np.ndarray), val
            num_success += 1
        except ray.exceptions.UnreconstructableError:
            num_evicted += 1
    # Some objects should have been evicted, and some should still be in the
    # object store.
    assert num_evicted > 0
    assert num_success > 0


def test_actor_restart():
    """Test actor restart when actor process is killed."""
    ray.init(
        _internal_config=json.dumps({
            "task_retry_delay_ms": 100,
        }), )

    @ray.remote(max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, delay=0):
            time.sleep(delay)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    results = [actor.increase.remote() for _ in range(100)]
    # Kill actor process, while the above task is still being executed.
    os.kill(pid, signal.SIGKILL)
    # Make sure that all tasks were executed in order before the actor's death.
    res = results.pop(0)
    i = 1
    while True:
        try:
            r = ray.get(res)
            if r != i:
                # Actor restarted without any failed tasks.
                break
            res = results.pop(0)
            i += 1
        except ray.exceptions.RayActorError:
            # Actor restarted.
            break
    # Find the first task to execute after the actor was restarted.
    while True:
        try:
            r = ray.get(res)
            break
        except ray.exceptions.RayActorError:
            res = results.pop(0)
            pass
    # Make sure that all tasks were executed in order after the actor's death.
    i = 1
    while True:
        r = ray.get(res)
        assert r == i
        if results:
            res = results.pop(0)
            i += 1
        else:
            break

    # Check that we can still call the actor.
    result = actor.increase.remote()
    assert ray.get(result) == r + 1

    # kill actor process one more time.
    results = [actor.increase.remote() for _ in range(100)]
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    # The actor has exceeded max restarts, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())

    # Create another actor.
    actor = RestartableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be restarted.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())
    ray.shutdown()


def test_actor_restart_with_retry():
    """Test actor restart when actor process is killed."""
    ray.init(
        _internal_config=json.dumps({
            "task_retry_delay_ms": 100,
        }), )

    @ray.remote(max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, delay=0):
            time.sleep(delay)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    results = [actor.increase.remote() for _ in range(100)]
    # Kill actor process, while the above task is still being executed.
    os.kill(pid, signal.SIGKILL)
    # Check that none of the tasks failed and the actor is restarted.
    seq = list(range(1, 101))
    results = ray.get(results)
    failed_task_index = None
    # Make sure that all tasks were executed in order before and after the
    # actor's death.
    for i, res in enumerate(results):
        if res != seq[0]:
            if failed_task_index is None:
                failed_task_index = i
            assert res + failed_task_index == seq[0]
        seq.pop(0)
    # Check that we can still call the actor.
    result = actor.increase.remote()
    assert ray.get(result) == results[-1] + 1

    # kill actor process one more time.
    results = [actor.increase.remote() for _ in range(100)]
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    # The actor has exceeded max restarts, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())

    # Create another actor.
    actor = RestartableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be restarted.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())
    ray.shutdown()


def test_actor_restart_on_node_failure(ray_start_cluster):
    config = json.dumps({
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_timeout_milliseconds": 100,
        "initial_reconstruction_timeout_milliseconds": 1000,
        "task_retry_delay_ms": 100,
    })
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _internal_config=config)
    # Node to place the actor.
    cluster.add_node(num_cpus=1, _internal_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1, max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be reconstructed at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

        def ready(self):
            return

    actor = RestartableActor.remote()
    ray.get(actor.ready.remote())
    results = [actor.increase.remote() for _ in range(100)]
    # Kill actor node, while the above task is still being executed.
    cluster.remove_node(cluster.list_all_nodes()[-1])
    cluster.add_node(num_cpus=1, _internal_config=config)
    cluster.wait_for_nodes()
    # Check that none of the tasks failed and the actor is restarted.
    seq = list(range(1, 101))
    results = ray.get(results)
    failed_task_index = None
    # Make sure that all tasks were executed in order before and after the
    # actor's death.
    for i, res in enumerate(results):
        elm = seq.pop(0)
        if res != elm:
            if failed_task_index is None:
                failed_task_index = i
            assert res + failed_task_index == elm
    # Check that we can still call the actor.
    result = ray.get(actor.increase.remote())
    assert result == 1 or result == results[-1] + 1


def test_actor_restart_without_task(ray_start_regular):
    """Test a dead actor can be restarted without sending task to it."""

    @ray.remote(max_restarts=1, resources={"actor": 1})
    class RestartableActor:
        def __init__(self):
            pass

        def get_pid(self):
            return os.getpid()

    @ray.remote(resources={"actor": 1})
    def probe():
        return

    # Returns whether the "actor" resource is available.
    def actor_resource_available():
        p = probe.remote()
        ready, _ = ray.wait([p], timeout=1)
        return len(ready) > 0

    ray.experimental.set_resource("actor", 1)
    actor = RestartableActor.remote()
    assert wait_for_condition(lambda: not actor_resource_available())
    # Kill the actor.
    pid = ray.get(actor.get_pid.remote())

    p = probe.remote()
    os.kill(pid, signal.SIGKILL)
    ray.get(p)
    assert wait_for_condition(lambda: not actor_resource_available())


def test_caller_actor_restart(ray_start_regular):
    """Test tasks from a restarted actor can be correctly processed
       by the receiving actor."""

    @ray.remote(max_restarts=1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self, actor):
            self.actor = actor

        def increase(self):
            return ray.get(self.actor.increase.remote())

        def get_pid(self):
            return os.getpid()

    @ray.remote(max_restarts=1)
    class Actor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

    remote_actor = Actor.remote()
    actor = RestartableActor.remote(remote_actor)
    # Call increase 3 times
    for _ in range(3):
        ray.get(actor.increase.remote())

    # kill the actor.
    # TODO(zhijunfu): use ray.kill instead.
    kill_actor(actor)

    # Check that we can still call the actor.
    assert ray.get(actor.increase.remote()) == 4


def test_caller_task_reconstruction(ray_start_regular):
    """Test a retried task from a dead worker can be correctly processed
       by the receiving actor."""

    @ray.remote(max_retries=5)
    def RetryableTask(actor):
        value = ray.get(actor.increase.remote())
        if value > 2:
            return value
        else:
            os._exit(0)

    @ray.remote(max_restarts=1)
    class Actor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

    remote_actor = Actor.remote()

    assert ray.get(RetryableTask.remote(remote_actor)) == 3


# NOTE(hchen): we set initial_reconstruction_timeout_milliseconds to 1s for
# this test. Because if this value is too small, suprious task reconstruction
# may happen and cause the test fauilure. If the value is too large, this test
# could be very slow. We can remove this once we support dynamic timeout.
@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_internal_config_map(
            initial_reconstruction_timeout_milliseconds=1000)
    ],
    indirect=True)
def test_multiple_actor_restart(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    # This test can be made more stressful by increasing the numbers below.
    # The total number of actors created will be
    # num_actors_at_a_time * num_nodes.
    num_nodes = 5
    num_actors_at_a_time = 3
    num_function_calls_at_a_time = 10

    worker_nodes = [
        cluster.add_node(
            num_cpus=3,
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 200,
                "num_heartbeats_timeout": 10,
            })) for _ in range(num_nodes)
    ]

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class SlowCounter:
        def __init__(self):
            self.x = 0

        def inc(self, duration):
            time.sleep(duration)
            self.x += 1
            return self.x

    # Create some initial actors.
    actors = [SlowCounter.remote() for _ in range(num_actors_at_a_time)]

    # Wait for the actors to start up.
    time.sleep(1)

    # This is a mapping from actor handles to object IDs returned by
    # methods on that actor.
    result_ids = collections.defaultdict(lambda: [])

    # In a loop we are going to create some actors, run some methods, kill
    # a raylet, and run some more methods.
    for node in worker_nodes:
        # Create some actors.
        actors.extend(
            [SlowCounter.remote() for _ in range(num_actors_at_a_time)])
        # Run some methods.
        for j in range(len(actors)):
            actor = actors[j]
            for _ in range(num_function_calls_at_a_time):
                result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))
        # Kill a node.
        cluster.remove_node(node)

        # Run some more methods.
        for j in range(len(actors)):
            actor = actors[j]
            for _ in range(num_function_calls_at_a_time):
                result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))

    # Get the results and check that they have the correct values.
    for _, result_id_list in result_ids.items():
        results = ray.get(result_id_list)
        for i, result in enumerate(results):
            if i == 0:
                assert result == 1
            else:
                assert result == results[i - 1] + 1 or result == 1


def kill_actor(actor):
    """A helper function that kills an actor process."""
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    wait_for_pid_to_exit(pid)


@pytest.mark.skip(reason="TODO: Actor checkpointing")
def test_checkpointing(ray_start_regular, ray_checkpointable_actor_cls):
    """Test actor checkpointing and restoring from a checkpoint."""
    actor = ray.remote(max_restarts=2)(ray_checkpointable_actor_cls).remote()
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
    # Kill actor again and check that restart still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


@pytest.mark.skip(reason="TODO: Actor checkpointing")
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

    cls = ray.remote(max_restarts=2)(RemoteCheckpointableActor)
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
    # Kill actor again and check that restart still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


@pytest.mark.skip(reason="TODO: Actor checkpointing")
def test_checkpointing_on_node_failure(ray_start_cluster_2_nodes,
                                       ray_checkpointable_actor_cls):
    """Test actor checkpointing on a remote node."""
    # Place the actor on the remote node.
    cluster = ray_start_cluster_2_nodes
    remote_node = list(cluster.worker_nodes)
    actor_cls = ray.remote(max_restarts=1)(ray_checkpointable_actor_cls)
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


@pytest.mark.skip(reason="TODO: Actor checkpointing")
def test_checkpointing_save_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to complete."""

    @ray.remote(max_restarts=2)
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
    # Kill actor again, and check that restart still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that the checkpoint error was pushed to the driver.
    wait_for_errors(ray_constants.CHECKPOINT_PUSH_ERROR, 1)


@pytest.mark.skip(reason="TODO: Actor checkpointing")
def test_checkpointing_load_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to load."""

    @ray.remote(max_restarts=2)
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
    # Kill actor again, and check that restart still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that the checkpoint error was pushed to the driver.
    wait_for_errors(ray_constants.CHECKPOINT_PUSH_ERROR, 1)


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


def test_bad_checkpointable_actor_class():
    """Test error raised if an actor class doesn't implement all abstract
    methods in the Checkpointable interface."""

    with pytest.raises(TypeError):

        @ray.remote
        class BadCheckpointableActor(ray.actor.Checkpointable):
            def should_checkpoint(self, checkpoint_context):
                return True


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

    def actor_dead():
        # Ping the actors and make sure the tasks complete.
        ping_ids = [actor.ping.remote() for actor in actors]
        unready = ping_ids[:]
        while unready:
            _, unready = ray.wait(unready, timeout=0)
            time.sleep(1)

        try:
            ray.get(ping_ids)
            return False
        except ray.exceptions.RayActorError:
            return True

    # Kill a node.
    cluster.remove_node(cluster.list_all_nodes()[-1])
    # Repeatedly submit tasks and call ray.wait until the exception for the
    # dead actor is received.
    assert wait_for_condition(actor_dead)

    # Create an actor on the local node that will call ray.wait in a loop.
    head_node_resource = "HEAD_NODE"
    ray.experimental.set_resource(head_node_resource, 1)

    @ray.remote(num_cpus=0, resources={head_node_resource: 1})
    class ParentActor:
        def __init__(self):
            pass

        def wait(self):
            return actor_dead()

        def ping(self):
            return

    # Repeatedly call ray.wait through the local actor until the exception for
    # the dead actor is received.
    parent_actor = ParentActor.remote()
    assert wait_for_condition(lambda: ray.get(parent_actor.wait.remote()))


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
