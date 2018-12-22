import pytest
import time

import ray
import ray.experimental.signal as signal

@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=4)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()

@ray.remote
class ActorSendSignal(object):
    def __init__(self):
        pass
    def send_signal(self, value):
        signal.send(signal.Signal(signal.SIG_USER, value))

@ray.remote
class ActorSendSignals(object):
    def __init__(self):
        pass
    def send_signals(self, value, count):
        for i in range(count):
            signal.send(signal.Signal(signal.SIG_USER, value + str(i)))
            time.sleep(0.1)

@ray.remote
class ActorGetSignals(object):
    def __init__(self):
        pass
    def get_signals(self, source_ids):
        return signal.receive(source_ids, timeout = 10)

@ray.remote
def task_send_signal(value):
    signal.send(signal.Signal(signal.SIG_USER, value))
    return

def test_task_to_driver(ray_start):
    """
    Send a signal from a task and another signal on behalf of the task
    from the driver. The driver gets both signals.

    """
    signal_value = "simple signal"
    oid = task_send_signal.remote(signal_value)
    ray.get(oid)
    time.sleep(2.0)
    result_list = signal.receive([signal.task_id(oid)], timeout = 10)
    assert len(result_list) == 2
    assert result_list[1][1].type() == signal.SIG_DONE


def test_send_signal_from_actor_to_driver(ray_start):
    """
    Send several signals from an actor, and receive them in the driver.
    """
    a = ActorSendSignal.remote()
    signal_value = "simple signal"
    count = 6
    for i in range(count):
        ray.get(a.send_signal.remote(signal_value + str(i)))
    time.sleep(1.0)
    result_list = signal.receive([a], timeout = 10)
    assert len(result_list) == count
    for i in range(count):
        assert signal_value + str(i) == (result_list[i][1]).value()

def test_send_signals_from_actor_to_driver(ray_start):
    """
    Send "count" signal at intervals of 100ms from an actor and get
    these signals in the driver.
    """
    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 20
    a.send_signals.remote(signal_value, count)
    received_count = 0;
    while True:
        result_list = signal.receive([a], timeout = 1)
        received_count += len(result_list)
        time.sleep(0.5)
        if (received_count == count):
            break
    assert True

@ray.remote
def f():
    return 1

def test_task_done(ray_start):
    oid = f.remote()
    #ray.get(oid)
    time.sleep(5)
    result_list = signal.receive([signal.task_id(oid)], timeout = 5)
    assert len(result_list) == 1
    assert result_list[0][1].type() == signal.SIG_DONE

@ray.remote
def f_crash():
    raise Exception("exception message")


def test_task_crash(ray_start):
    """
    Get an error when ray.get() is called on the return of a failed task.
    """
    oid = f_crash.remote()
    try:
        ray.get(oid)
    except ray.worker.RayTaskError as err:
        print("Error = ", err)
    finally:
        result_list = signal.receive([(oid)], timeout = 5)
        assert len(result_list) == 1
        assert result_list[0][1].type() == signal.SIG_ERROR
        print("error = ", result_list[0][1].value())

def test_task_crash_without_get(ray_start):
    """
    Get an error when task failed.
    """
    oid = f_crash.remote()
    time.sleep(5)
    result_list = signal.receive([(oid)], timeout = 5)
    assert len(result_list) == 1
    assert result_list[0][1].type() == signal.SIG_ERROR
    print("error = ", result_list[0][1].value())


@ray.remote
class a_crash(object):
    def __init__(self):
        pass
    def m(slef):
        return 1
    def m_crash(self):
        raise Exception("exception message")

def test_actor_crash(ray_start):
    """
    Get an error when ray.get() is called on a return parameter of a method that filed.
    """
    a = a_crash.remote()
    try:
        ray.get(a.m_crash.remote())
    except ray.worker.RayTaskError as err:
        print("Error = ", err)
    finally:
        result_list = signal.receive([a], timeout = 5)
        assert len(result_list) == 1
        assert result_list[0][1].type() == signal.SIG_ERROR
        print("error = ", result_list[0][1].value())


@ray.remote
class a_crash_init(object):
    def __init__(self):
        raise Exception("exception message")
    def m(slef):
        return 1

def test_actor_crash_init(ray_start):
    """
    Get an error when an actor's __init__ failed.
    """
    # Using try and catch won't catch the exception in the __init__.
    a = a_crash_init.remote()
    time.sleep(10)
    result_list = signal.receive([a], timeout = 5)
    assert len(result_list) == 1
    assert result_list[0][1].type() == signal.SIG_ERROR
    print("error = ", result_list[0][1].value())


def test_actor_crash_init2(ray_start):
    """
    Get errors when (1) __init__ fails, and (2) subsequently when ray.get() is
    called on the return parameter of another method of the actor.
    """
    a = a_crash_init.remote()
    try:
        ray.get(a.m.remote())
    except ray.worker.RayTaskError as err:
        print("Error = ", err)
    finally:
        time.sleep(5)
        result_list = signal.receive([a], timeout = 5)
        assert len(result_list) == 2
        assert result_list[0][1].type() == signal.SIG_ERROR
        print("error = ", result_list[0][1].value())

def test_actor_crash_init3(ray_start):
    """
    Get errors when (1) __init__ fails, and (2) subsequently when
    another method of the actor is invoked.
    """
    a = a_crash_init.remote()
    a.m.remote()
    time.sleep(5)
    result_list = signal.receive([a], timeout = 5)
    assert len(result_list) == 2
    assert result_list[0][1].type() == signal.SIG_ERROR
    print("error = ", result_list[0][1].value())


def test_send_signals_from_actor_to_actor(ray_start):
    """
    Send "count" signal at intervals of 100ms from two actors and get
    these signals in another actor.
    """
    @ray.remote
    class ActorSendSignals(object):
        def __init__(self):
            pass
        def send_signals(self, value, count):
            for i in range(count):
                signal.send(signal.Signal(signal.SIG_USER, value + str(i)))
                time.sleep(0.1)

    @ray.remote
    class ActorGetSignalsAll(object):
        def __init__(self):
            self.received_signals = []
        def register_handle(self, handle):
            self.this_actor = handle
        def get_signals(self, source_ids, count):
            new_signals = signal.receive(source_ids, timeout = 10)
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
    assert received_count == 2*count

def test_forget(ray_start):
    """
    Send "count" signals on behalf of an actor, then ignore all these signals,
    and then send anther "count" signals on behalf of the same actor. Then
    show that the driver only gets the last "count" signals.
    """
    a = ActorSendSignals.remote()
    signal_value = "simple signal"
    count = 5
    ray.get(a.send_signals.remote(signal_value, count))
    signal.forget([a])
    ray.get(a.send_signals.remote(signal_value, count))
    result_list = signal.receive([a], timeout = 10)
    assert len(result_list) == count
    #for i in range(count):
    #    assert signal_value + str(i) == (result_list[i][1]).value()
