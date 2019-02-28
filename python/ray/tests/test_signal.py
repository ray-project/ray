import pytest

import ray
import ray.experimental.signal as signal

import json
import os

import ray.tests.cluster_utils


class UserSignal(signal.Signal):
    def __init__(self, value):
        self.value = value


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=4)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def receive_all_signals(sources, timeout):
    # Get all signals from sources, until there is no signal for a time
    # period of timeout.

    results = []
    while True:
        r = signal.receive(sources, timeout=timeout)
        if len(r) == 0:
            return results
        else:
            results.extend(r)

@pytest.fixture()
def two_node_cluster():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    })
    cluster = ray.tests.cluster_utils.Cluster()
    for _ in range(2):
        remote_node = cluster.add_node(
            num_cpus=1, _internal_config=internal_config)
    ray.init(redis_address=cluster.redis_address)
    yield cluster, remote_node

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()

def test_task_to_driver(ray_start):
    # Send a signal from a task to the driver.

    @ray.remote
    def task_send_signal(value):
        signal.send(UserSignal(value))
        return

    signal_value = "simple signal"
    object_id = task_send_signal.remote(signal_value)
    result_list = signal.receive([object_id], timeout=10)
    print(result_list[0][1])
    assert len(result_list) == 1


def test_send_signal_from_actor_to_driver(ray_start):
    # Send several signals from an actor, and receive them in the driver.

    @ray.remote
    class ActorSendSignal(object):
        def __init__(self):
            pass

        def send_signal(self, value):
            signal.send(UserSignal(value))

    a = ActorSendSignal.remote()
    signal_value = "simple signal"
    count = 6
    for i in range(count):
        ray.get(a.send_signal.remote(signal_value + str(i)))

    result_list = receive_all_signals([a], timeout=5)
    assert len(result_list) == count
    for i in range(count):
        assert signal_value + str(i) == result_list[i][1].value


def test_send_signals_from_actor_to_driver(ray_start):
    # Send "count" signal at intervals from an actor and get
    # these signals in the driver.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass

        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 20
    a.send_signals.remote(signal_value, count)
    received_count = 0
    while True:
        result_list = signal.receive([a], timeout=5)
        received_count += len(result_list)
        if (received_count == count):
            break
    assert True


def test_task_crash(ray_start):
    # Get an error when ray.get() is called on the return of a failed task.

    @ray.remote
    def crashing_function():
        raise Exception("exception message")

    object_id = crashing_function.remote()
    try:
        ray.get(object_id)
    except Exception as e:
        assert type(e) == ray.exceptions.RayTaskError
    finally:
        result_list = signal.receive([object_id], timeout=5)
        assert len(result_list) == 1
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_task_crash_without_get(ray_start):
    # Get an error when task failed.

    @ray.remote
    def crashing_function():
        raise Exception("exception message")

    object_id = crashing_function.remote()
    result_list = signal.receive([object_id], timeout=5)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash(ray_start):
    # Get an error when ray.get() is called on a return parameter
    # of a method that failed.

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def crash(self):
            raise Exception("exception message")

    a = Actor.remote()
    try:
        ray.get(a.crash.remote())
    except Exception as e:
        assert type(e) == ray.exceptions.RayTaskError
    finally:
        result_list = signal.receive([a], timeout=5)
        assert len(result_list) == 1
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init(ray_start):
    # Get an error when an actor's __init__ failed.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")

        def m(self):
            return 1

    # Do not catch the exception in the __init__.
    a = ActorCrashInit.remote()
    result_list = signal.receive([a], timeout=5)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init2(ray_start):
    # Get errors when (1) __init__ fails, and (2) subsequently when
    # ray.get() is called on the return parameter of another method
    # of the actor.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")

        def method(self):
            return 1

    a = ActorCrashInit.remote()
    try:
        ray.get(a.method.remote())
    except Exception as e:
        assert type(e) == ray.exceptions.RayTaskError
    finally:
        result_list = receive_all_signals([a], timeout=5)
        assert len(result_list) == 2
        assert type(result_list[0][1]) == signal.ErrorSignal


def test_actor_crash_init3(ray_start):
    # Get errors when (1) __init__ fails, and (2) subsequently when
    # another method of the actor is invoked.

    @ray.remote
    class ActorCrashInit(object):
        def __init__(self):
            raise Exception("exception message")

        def method(self):
            return 1

    a = ActorCrashInit.remote()
    a.method.remote()
    result_list = signal.receive([a], timeout=10)
    assert len(result_list) == 1
    assert type(result_list[0][1]) == signal.ErrorSignal


def test_send_signals_from_actor_to_actor(ray_start):
    # Send "count" signal at intervals of 100ms from two actors and get
    # these signals in another actor.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass

        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    @ray.remote
    class ActorGetSignalsAll(object):
        def __init__(self):
            self.received_signals = []

        def register_handle(self, handle):
            self.this_actor = handle

        def get_signals(self, source_ids, count):
            new_signals = receive_all_signals(source_ids, timeout=5)
            for s in new_signals:
                self.received_signals.append(s)
            if len(self.received_signals) < count:
                self.this_actor.get_signals.remote(source_ids, count)
            else:
                return

        def get_count(self):
            return len(self.received_signals)

    a1 = ActorSendSignals.remote()
    a2 = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 20
    ray.get(a1.send_signals.remote(signal_value, count))
    ray.get(a2.send_signals.remote(signal_value, count))

    b = ActorGetSignalsAll.remote()
    ray.get(b.register_handle.remote(b))
    b.get_signals.remote([a1, a2], count)
    received_count = ray.get(b.get_count.remote())
    assert received_count == 2 * count


def test_forget(ray_start):
    # Send "count" signals on behalf of an actor, then ignore all these
    # signals, and then send anther "count" signals on behalf of the same
    # actor. Then show that the driver only gets the last "count" signals.

    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass

        def send_signals(self, value, count):
            for i in range(count):
                signal.send(UserSignal(value + str(i)))

    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 5
    ray.get(a.send_signals.remote(signal_value, count))
    signal.forget([a])
    ray.get(a.send_signals.remote(signal_value, count))
    result_list = receive_all_signals([a], timeout=5)
    assert len(result_list) == count


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

        def local_plasma(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

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

            with open(filename, "r") as f:
                lines = f.readlines()
                checkpoint_id, value = lines[-1].split(" ")
                self.value = int(value)
                self.resumed_from_checkpoint = True
                checkpoint_id = ray.ActorCheckpointID(
                    ray.utils.hex_to_binary(checkpoint_id))
                assert any(checkpoint_id == checkpoint.checkpoint_id
                           for checkpoint in available_checkpoints)
                return checkpoint_id

        def checkpoint_expired(self, actor_id, checkpoint_id):
            pass

    return CheckpointableActor


def test_checkpointing_on_node_failure(two_node_cluster,
                                       ray_checkpointable_actor_cls):
    """Test actor checkpointing on a remote node."""
    # Place the actor on the remote node.
    cluster, remote_node = two_node_cluster
    actor_cls = ray.remote(max_reconstructions=1)(ray_checkpointable_actor_cls)
    actor = actor_cls.remote()
    while (ray.get(actor.local_plasma.remote()) !=
           remote_node.plasma_store_socket_name):
        actor = actor_cls.remote()

    # Call increase several times.
    expected = 0
    for _ in range(6):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    cluster.remove_node(remote_node)
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True
