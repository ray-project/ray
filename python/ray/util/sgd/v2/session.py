import queue
import time
import threading
from typing import Callable

from ray.util.sgd.v2.constants import TIME_THIS_ITER_S, RESULT_FETCH_TIMEOUT
from ray.util.sgd.v2.utils import PropagatingThread


class Session:
    """Holds information for training on each worker."""

    def __init__(self, training_func: Callable, world_rank: int):
        # The Thread object that is running the training function.
        self.training_thread = PropagatingThread(
            target=training_func, daemon=True)
        self.world_rank = world_rank

        # This lock is used to control the execution of the training thread.
        self.continue_lock = threading.Semaphore(0)

        # Queue for sending results across threads.
        self.result_queue = queue.Queue(1)

        self.last_report_time = time.time()

        self.ignore_report = False
        self.training_started = False

    def start(self):
        """Starts the training thread."""
        self.training_started = True
        self.training_thread.start()

    def finish(self):
        """Finishes the training thread.

        Either returns the output from training or raises any Exception from
        training.

        """
        # Ignore all future sgd.report calls.
        self.ignore_report = True

        # Release lock so that training will continue even if
        # fetch_next_result is not exhausted.
        self.continue_lock.release()

        # Wait for training to finish.
        # This will raise any errors that occur during training, including
        # SystemError
        func_output = self.training_thread.join()
        # If training finished successfully, then return results.
        return func_output

    def get_next(self):
        """Gets next result from the queue."""
        if not self.training_started:
            raise RuntimeError("Please call start before calling get_next.")
        result = None
        # While training is still ongoing, attempt to get the result.
        while result is None and self.training_thread.is_alive():
            try:
                result = self.result_queue.get(
                    block=True, timeout=RESULT_FETCH_TIMEOUT)
            except queue.Empty:
                pass

        # If no result was found, then the runner must no longer be alive.
        if result is None:
            # Try one last time to fetch results in case results were
            # reported in between the time of the last check and the
            # termination of the thread runner.
            try:
                result = self.result_queue.get(
                    block=False, timeout=RESULT_FETCH_TIMEOUT)
            except queue.Empty:
                pass

        # Release the lock to trigger training to continue.
        self.continue_lock.release()

        # Return None if there are no more results to fetch.
        return result

    def report(self, **kwargs):
        """Adds kwargs to the queue to be consumed by main thread."""
        if self.ignore_report:
            return
        current_time = time.time()
        time_this_iter = current_time - self.last_report_time
        if TIME_THIS_ITER_S not in kwargs:
            kwargs[TIME_THIS_ITER_S] = time_this_iter
        self.last_report_time = current_time

        # Add result to a thread-safe queue.
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
