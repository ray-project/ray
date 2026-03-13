import threading
from types import SimpleNamespace
from typing import List, Tuple

import ray
import ray._private.worker as core_worker


class _DummyDeduplicator:
    """Minimal stand-in for stdout/stderr deduplicators used in tests.

    The real deduplicators expose a ``flush`` method that yields log batches.
    For these edge-case tests we only care that ``disconnect`` can iterate
    over the return value without error, so this implementation always returns
    an empty sequence.
    """

    def __init__(self):
        self.flushed: List[Tuple] = []

    def flush(self):
        self.flushed.append(tuple())
        return []


class _DummyDispatcher:
    """Capture calls to ``remove_handler`` made during disconnect."""

    def __init__(self):
        self.removed_handlers: List[str] = []

    def remove_handler(self, name: str):
        self.removed_handlers.append(name)


class _DummySubscriber:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _DummyWorker:
    """Lightweight stand-in for ``global_worker`` used to exercise ``disconnect``.

    Only the attributes and methods touched by ``disconnect`` are implemented.
    """

    def __init__(self):
        self.threads_stopped = threading.Event()
        self.gcs_error_subscriber = _DummySubscriber()
        self.gcs_log_subscriber = _DummySubscriber()
        # listener_thread and logger_thread are populated by individual tests.
        self.listener_thread = None
        self.logger_thread = None
        self.job_logging_config = None
        self.serialization_context_map = {}
        self.node = object()
        self._is_connected = True

    @property
    def connected(self) -> bool:
        """Mirror the real Worker's ``connected`` property backed by _is_connected."""
        return self._is_connected

    def set_is_connected(self, value: bool):
        self._is_connected = value


def _install_dummy_worker_and_logging():
    """Swap in dummy worker and logging utilities, returning originals."""
    originals = SimpleNamespace(
        global_worker=core_worker.global_worker,
        stdout_deduplicator=core_worker.stdout_deduplicator,
        stderr_deduplicator=core_worker.stderr_deduplicator,
        dispatcher=core_worker.global_worker_stdstream_dispatcher,
    )

    dummy_worker = _DummyWorker()
    core_worker.global_worker = dummy_worker
    core_worker.stdout_deduplicator = _DummyDeduplicator()
    core_worker.stderr_deduplicator = _DummyDeduplicator()
    core_worker.global_worker_stdstream_dispatcher = _DummyDispatcher()

    return dummy_worker, originals


def _restore_worker_and_logging(originals):
    core_worker.global_worker = originals.global_worker
    core_worker.stdout_deduplicator = originals.stdout_deduplicator
    core_worker.stderr_deduplicator = originals.stderr_deduplicator
    core_worker.global_worker_stdstream_dispatcher = originals.dispatcher


def test_disconnect_does_not_join_current_listener_thread():
    """Calling ``disconnect`` from the listener thread must not deadlock.

    This test simulates a background thread that both represents the
    ``listener_thread`` and invokes ``disconnect``. The implementation should
    detect that the current thread is the listener and skip the join.
    """
    dummy_worker, originals = _install_dummy_worker_and_logging()

    try:
        # The target function runs inside the background thread. Inside that
        # thread, ``threading.current_thread()`` must equal the stored
        # ``listener_thread`` instance when ``disconnect`` is called.
        def target():
            # At this point, ``threading.current_thread()`` is the thread
            # object created below and assigned to ``listener_thread``.
            core_worker.disconnect()

        t = threading.Thread(target=target, name="dummy-listener-thread")
        # Expose the same thread object via the worker so ``disconnect`` can
        # compare it against ``threading.current_thread()``.
        dummy_worker.listener_thread = t

        t.start()
        t.join(timeout=5)

        # If the join on the listener thread were not guarded, this test would
        # deadlock. Verifying that the thread has finished is sufficient.
        assert not t.is_alive()
        assert dummy_worker._is_connected is False
        assert dummy_worker.listener_thread is None
    finally:
        _restore_worker_and_logging(originals)


def test_disconnect_does_not_join_current_logger_thread():
    """Calling ``disconnect`` from the logger thread must not deadlock."""
    dummy_worker, originals = _install_dummy_worker_and_logging()

    try:
        def target():
            core_worker.disconnect()

        t = threading.Thread(target=target, name="dummy-logger-thread")
        dummy_worker.logger_thread = t

        t.start()
        t.join(timeout=5)

        assert not t.is_alive()
        assert dummy_worker._is_connected is False
        assert dummy_worker.logger_thread is None
    finally:
        _restore_worker_and_logging(originals)


def test_shutdown_safe_without_init_and_repeated(shutdown_only):
    """``ray.shutdown()`` should be safe without prior ``ray.init()`` and idempotent.

    Long-running applications may invoke shutdown defensively multiple times
    (for example, during layered cleanup in miner-style processes). This test
    ensures those calls are safe no-ops when Ray is not initialized.
    """
    # Ensure Ray is not initialized at the start of the test.
    if ray.is_initialized():
        ray.shutdown()

    # Multiple shutdown calls without init should not raise.
    ray.shutdown()
    ray.shutdown()

    # Normal init / shutdown cycle should continue to work afterwards.
    ray.init(num_cpus=1, include_dashboard=False)
    ray.shutdown()


def test_disconnect_idempotent_on_dummy_worker():
    """Calling ``disconnect`` multiple times should be safe and idempotent.

    This exercises the disconnect logic directly without going through the
    public shutdown path, ensuring repeated invocations do not raise and leave
    the dummy worker in a consistently disconnected state.
    """
    dummy_worker, originals = _install_dummy_worker_and_logging()

    try:
        # First disconnect should transition the worker to a disconnected state.
        core_worker.disconnect()
        assert dummy_worker._is_connected is False
        assert dummy_worker.node is None

        # Subsequent disconnect calls should be harmless no-ops.
        core_worker.disconnect()
        core_worker.disconnect()
        assert dummy_worker._is_connected is False
    finally:
        _restore_worker_and_logging(originals)


def test_shutdown_multiple_cycles(shutdown_only):
    """Verify that repeated init/shutdown cycles complete without error.

    Long-running miner processes may reconfigure and restart Ray several
    times within the same OS process. This test ensures a few basic cycles
    succeed without leaking state or raising exceptions.
    """
    for _ in range(3):
        assert not ray.is_initialized()
        ray.init(num_cpus=1, include_dashboard=False)
        assert ray.is_initialized()
        ray.shutdown()
        assert not ray.is_initialized()


