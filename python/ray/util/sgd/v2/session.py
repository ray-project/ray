import queue
import time
import threading

from ray.util.sgd.v2.constants import TIME_THIS_ITER_S


class Session:
    """Holds information for training on each worker."""

    def __init__(self, training_thread: threading.Thread, world_rank: int):
        # The Thread object that is running the training function.
        self.training_thread = training_thread
        self.world_rank = world_rank

        # This lock is used to control the execution of the training thread.
        self.continue_lock = threading.Semaphore(0)

        # Queue for sending results across threads.
        self.result_queue = queue.Queue(1)

        self.last_report_time = time.time()

    def report(self, **kwargs):
        """Adds kwargs to the queue to be consumed by main thread."""
        current_time = time.time()
        time_this_iter = current_time - self.last_report_time
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = time_this_iter

        # Add result to a thread-safe queue.

        # TODO: Can this ever hang? Yes if the queue is already full.
        self.result_queue.put(kwargs.copy(), block=True)

        # Acquire lock to stop the training thread until main thread
        # triggers resume.
        self.continue_lock.acquire()


_session = None


def init_session(*args, **kwargs) -> None:
    global _session
    if _session:
        raise ValueError("An SGD session is already in use. Do not call "
                         "`init_session()` manually.")
    _session = Session(*args, **kwargs)


def get_session() -> Session:
    global _session
    if _session is None or not isinstance(_session, Session):
        raise ValueError("Trying to access an SGD session that has not been "
                         "initialized yet. SGD functions like `sgd.report()` "
                         "should only be called from inside the training "
                         "function.")
    return _session


def shutdown_session():
    """Shuts down the initialized session."""
    global _session
    _session = None


def report(**kwargs) -> None:
    """Reports all keyword arguments to SGD as intermediate results.

    .. code-block:: python

        import time
        from ray.util import sgd

        def train_func():
            for iter in range(100):
                time.sleep(1)
                sgd.report(hello="world")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be reported by SGD.
            If callbacks are provided, they are executed on these
            intermediate results.
    """
    session = get_session()
    session.report(**kwargs)


def world_rank() -> int:
    """Get the world rank of this worker.

    .. code-block:: python

        import time
        from ray.util import sgd

        def train_func():
            for iter in range(100):
                time.sleep(1)
                if sgd.world_rank() == 0:
                    print("Worker 0")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    """
    session = get_session()
    return session.world_rank
