import contextlib
from collections import defaultdict, namedtuple
from datetime import datetime
from typing import Any, List, Optional

from dask.callbacks import Callback

import ray

# The names of the Ray-specific callbacks. These are the kwarg names that
# RayDaskCallback will accept on construction, and is considered the
# source-of-truth for what Ray-specific callbacks exist.
CBS = (
    "ray_presubmit",
    "ray_postsubmit",
    "ray_pretask",
    "ray_posttask",
    "ray_postsubmit_all",
    "ray_finish",
)
# The Ray-specific callback method names for RayDaskCallback.
CB_FIELDS = tuple("_" + field for field in CBS)
# The Ray-specific callbacks that we do _not_ wish to drop from RayCallbacks
# if not given on a RayDaskCallback instance (will be filled with None
# instead).
CBS_DONT_DROP = {"ray_pretask", "ray_posttask"}

# The Ray-specific callbacks for a single RayDaskCallback.
RayCallback = namedtuple("RayCallback", " ".join(CBS))

# The Ray-specific callbacks for one or more RayDaskCallbacks.
RayCallbacks = namedtuple("RayCallbacks", " ".join([field + "_cbs" for field in CBS]))


class RayDaskCallback(Callback):
    """
    Extends Dask's `Callback` class with Ray-specific hooks. When instantiating
    or subclassing this class, both the normal Dask hooks (e.g. pretask,
    posttask, etc.) and the Ray-specific hooks can be provided.

    See `dask.callbacks.Callback` for usage.

    Caveats: Any Dask-Ray scheduler must bring the Ray-specific callbacks into
    context using the `local_ray_callbacks` context manager, since the built-in
    `local_callbacks` context manager provided by Dask isn't aware of this
    class.
    """

    # Set of active Ray-specific callbacks.
    ray_active = set()

    def __init__(self, **kwargs):
        for cb in CBS:
            cb_func = kwargs.pop(cb, None)
            if cb_func is not None:
                setattr(self, "_" + cb, cb_func)

        super().__init__(**kwargs)

    @property
    def _ray_callback(self):
        return RayCallback(*[getattr(self, field, None) for field in CB_FIELDS])

    def __enter__(self):
        self._ray_cm = add_ray_callbacks(self)
        self._ray_cm.__enter__()
        super().__enter__()
        return self

    def __exit__(self, *args):
        super().__exit__(*args)
        self._ray_cm.__exit__(*args)

    def register(self):
        type(self).ray_active.add(self._ray_callback)
        super().register()

    def unregister(self):
        type(self).ray_active.remove(self._ray_callback)
        super().unregister()

    def _ray_presubmit(self, task, key, deps) -> Optional[Any]:
        """Run before submitting a Ray task.

        If this callback returns a non-`None` value, Ray does _not_ create
        a task and uses this value as the would-be task's result value.

        Args:
            task: A Dask task, where the first tuple item is
                the task function, and the remaining tuple items are
                the task arguments, which are either the actual argument values,
                or Dask keys into the deps dictionary whose
                corresponding values are the argument values.
            key: The Dask graph key for the given task.
            deps: The dependencies of this task.

        Returns:
            Either None, in which case Ray submits a task, or
            a non-None value, in which case Ray task doesn't submit
            a task and uses this return value as the
            would-be task result value.
        """
        pass

    def _ray_postsubmit(self, task, key, deps, object_ref: ray.ObjectRef):
        """Run after submitting a Ray task.

        Args:
            task: A Dask task, where the first tuple item is
                the task function, and the remaining tuple items are
                the task arguments, which are either the actual argument values,
                or Dask keys into the deps dictionary whose
                corresponding values are the argument values.
            key: The Dask graph key for the given task.
            deps: The dependencies of this task.
            object_ref: The object reference for the
                return value of the Ray task.

        """
        pass

    def _ray_pretask(self, key, object_refs: List[ray.ObjectRef]):
        """Run before executing a Dask task within a Ray task.

        This method executes after Ray submits the task within a Ray
        worker. Ray passes the return value of this task to the
        _ray_posttask callback, if provided.

        Args:
            key: The Dask graph key for the Dask task.
            object_refs: The object references
                for the arguments of the Ray task.

        Returns:
            A value that Ray passes to the corresponding
            _ray_posttask callback, if the callback is defined.
        """
        pass

    def _ray_posttask(self, key, result, pre_state):
        """Run after executing a Dask task within a Ray task.

        This method executes within a Ray worker. This callback receives the
        return value of the _ray_pretask callback, if provided.

        Args:
            key: The Dask graph key for the Dask task.
            result: The task result value.
            pre_state: The return value of the corresponding
                _ray_pretask callback, if said callback is defined.
        """
        pass

    def _ray_postsubmit_all(self, object_refs: List[ray.ObjectRef], dsk):
        """Run after Ray submits all tasks.

        Args:
            object_refs: The object references
                for the output (leaf) Ray tasks of the task graph.
            dsk: The Dask graph.
        """
        pass

    def _ray_finish(self, result):
        """Run after Ray finishes executing all Ray tasks and returns the final
        result.

        Args:
          result: The final result (output) of the Dask
              computation, before any repackaging is done by
              Dask collection-specific post-compute callbacks.
        """
        pass


class add_ray_callbacks:
    def __init__(self, *callbacks):
        self.callbacks = [normalize_ray_callback(c) for c in callbacks]
        RayDaskCallback.ray_active.update(self.callbacks)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for c in self.callbacks:
            RayDaskCallback.ray_active.discard(c)


def normalize_ray_callback(cb):
    if isinstance(cb, RayDaskCallback):
        return cb._ray_callback
    elif isinstance(cb, RayCallback):
        return cb
    else:
        raise TypeError(
            "Callbacks must be either 'RayDaskCallback' or 'RayCallback' namedtuple"
        )


def unpack_ray_callbacks(cbs):
    """Take an iterable of callbacks, return a list of each callback."""
    if cbs:
        # Only drop callback methods that aren't in CBS_DONT_DROP.
        return RayCallbacks(
            *(
                [cb for cb in cbs_ if cb or CBS[idx] in CBS_DONT_DROP] or None
                for idx, cbs_ in enumerate(zip(*cbs))
            )
        )
    else:
        return RayCallbacks(*([()] * len(CBS)))


@contextlib.contextmanager
def local_ray_callbacks(callbacks=None):
    """
    Allows Dask-Ray callbacks to work with nested schedulers.

    Callbacks will only be used by the first started scheduler they encounter.
    This means that only the outermost scheduler will use global callbacks.
    """
    global_callbacks = callbacks is None
    if global_callbacks:
        callbacks, RayDaskCallback.ray_active = (RayDaskCallback.ray_active, set())
    try:
        yield callbacks or ()
    finally:
        if global_callbacks:
            RayDaskCallback.ray_active = callbacks


class ProgressBarCallback(RayDaskCallback):
    def __init__(self):
        @ray.remote
        class ProgressBarActor:
            def __init__(self):
                self._init()

            def submit(self, key, deps, now):
                for dep in deps.keys():
                    self.deps[key].add(dep)
                self.submitted[key] = now
                self.submission_queue.append((key, now))

            def task_scheduled(self, key, now):
                self.scheduled[key] = now

            def finish(self, key, now):
                self.finished[key] = now

            def result(self):
                return len(self.submitted), len(self.finished)

            def report(self):
                result = defaultdict(dict)
                for key, finished in self.finished.items():
                    submitted = self.submitted[key]
                    scheduled = self.scheduled[key]
                    # deps = self.deps[key]
                    result[key]["execution_time"] = (
                        finished - scheduled
                    ).total_seconds()
                    # Calculate the scheduling time.
                    # This is inaccurate.
                    # We should subtract scheduled - (last dep completed).
                    # But currently it is not easy because
                    # of how getitem is implemented in dask on ray sort.
                    result[key]["scheduling_time"] = (
                        scheduled - submitted
                    ).total_seconds()
                result["submission_order"] = self.submission_queue
                return result

            def ready(self):
                pass

            def reset(self):
                self._init()

            def _init(self):
                self.submission_queue = []
                self.submitted = defaultdict(None)
                self.scheduled = defaultdict(None)
                self.finished = defaultdict(None)
                self.deps = defaultdict(set)

        try:
            self.pb = ray.get_actor("_dask_on_ray_pb")
            ray.get(self.pb.reset.remote())
        except ValueError:
            self.pb = ProgressBarActor.options(name="_dask_on_ray_pb").remote()
            ray.get(self.pb.ready.remote())

    def _ray_postsubmit(self, task, key, deps, object_ref):
        # Indicate the dask task is submitted.
        self.pb.submit.remote(key, deps, datetime.now())

    def _ray_pretask(self, key, object_refs):
        self.pb.task_scheduled.remote(key, datetime.now())

    def _ray_posttask(self, key, result, pre_state):
        # Indicate the dask task is finished.
        self.pb.finish.remote(key, datetime.now())

    def _ray_finish(self, result):
        print("All tasks are completed.")
