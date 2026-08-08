import logging
from typing import Any, Dict, Optional

from joblib import Parallel
from joblib._parallel_backends import MultiprocessingBackend
from joblib.pool import PicklingPool

import ray
from ray._common.usage import usage_lib
from ray.util.multiprocessing.pool import Pool

logger = logging.getLogger(__name__)


class RayBackend(MultiprocessingBackend):
    """Ray backend uses ray, a system for scalable distributed computing.
    More info about Ray is available here: https://docs.ray.io.
    """

    def __init__(
        self,
        nesting_level: Optional[int] = None,
        inner_max_num_threads: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        # Autoscaling params (forwarded to Pool via configure). Supplying any
        # of them makes the pool autoscale; None means "unspecified" so the
        # default fixed-size pool keeps its current behavior.
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        idle_timeout_s: Optional[float] = None,
        **kwargs
    ):
        """``ray_remote_args`` will be used to configure Ray Actors
        making up the pool. The autoscaling params are stored here and
        forwarded to ``Pool`` in ``configure``."""
        usage_lib.record_library_usage("util.joblib")

        self.ray_remote_args = ray_remote_args
        self._size_kwargs = dict(
            min_size=min_size,
            max_size=max_size,
            initial_size=initial_size,
            idle_timeout_s=idle_timeout_s,
        )
        super().__init__(
            nesting_level=nesting_level,
            inner_max_num_threads=inner_max_num_threads,
            **kwargs
        )

    # ray_remote_args is used both in __init__ and configure to allow for it to be
    # set in both `parallel_backend` and `Parallel` respectively

    def configure(
        self,
        n_jobs: int = 1,
        parallel: Optional[Parallel] = None,
        prefer: Optional[str] = None,
        require: Optional[str] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **memmappingpool_args
    ):
        """Make Ray Pool the father class of PicklingPool. PicklingPool is a
        father class that inherits Pool from multiprocessing.pool. The next
        line is a patch, which changes the inheritance of Pool to be from
        ray.util.multiprocessing.pool.

        ``ray_remote_args`` will be used to configure Ray Actors making up the pool.
        This will override ``ray_remote_args`` set during initialization.
        """
        PicklingPool.__bases__ = (Pool,)
        """Use all available resources when n_jobs == -1. Must set RAY_ADDRESS
        variable in the environment or run ray.init(address=..) to run on
        multiple nodes.
        """
        if n_jobs == -1:
            if not ray.is_initialized():
                import os

                if "RAY_ADDRESS" in os.environ:
                    logger.info(
                        "Connecting to ray cluster at address='{}'".format(
                            os.environ["RAY_ADDRESS"]
                        )
                    )
                else:
                    logger.info("Starting local ray cluster")
                ray.init()
            ray_cpus = max(int(ray.cluster_resources().get("CPU", 1)), 1)
            n_jobs = ray_cpus

        # Forward autoscaling params through
        # MemmappingPool -> PicklingPool -> Pool.__init__. Supplying any of
        # them enables Pool autoscaling. joblib's n_jobs remains the
        # concurrency limit; max_size may only lower that limit.
        size_kwargs = dict(self._size_kwargs)
        if any(v is not None for v in size_kwargs.values()):
            pool_size = self.effective_n_jobs(n_jobs)
            configured_max = size_kwargs["max_size"]
            size_kwargs["max_size"] = (
                pool_size if configured_max is None else min(configured_max, pool_size)
            )
            if size_kwargs["min_size"] is not None:
                size_kwargs["min_size"] = min(
                    size_kwargs["min_size"], size_kwargs["max_size"]
                )
            if size_kwargs["initial_size"] is not None:
                size_kwargs["initial_size"] = min(
                    size_kwargs["initial_size"], size_kwargs["max_size"]
                )
            memmappingpool_args.update(
                {k: v for k, v in size_kwargs.items() if v is not None}
            )

        eff_n_jobs = super(RayBackend, self).configure(
            n_jobs,
            parallel,
            prefer,
            require,
            ray_remote_args=ray_remote_args
            if ray_remote_args is not None
            else self.ray_remote_args,
            **memmappingpool_args
        )
        return eff_n_jobs

    def effective_n_jobs(self, n_jobs):
        eff_n_jobs = super(RayBackend, self).effective_n_jobs(n_jobs)
        if n_jobs == -1 and ray.is_initialized():
            # Resolve to the cluster CPU count only when Ray is already up;
            # if this is called before configure()/ray.init() (e.g. via
            # joblib.effective_n_jobs()), fall back to joblib's default rather
            # than crashing on cluster_resources().
            eff_n_jobs = max(int(ray.cluster_resources().get("CPU", 1)), 1)
        return eff_n_jobs
