import gc
import inspect
import logging
from typing import Any, Dict, Optional

from joblib import Parallel
from joblib._parallel_backends import MultiprocessingBackend
from joblib.pool import MemmappingPool

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
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        idle_timeout_s: Optional[float] = None,
        **kwargs,
    ):
        """``ray_remote_args`` will be used to configure Ray Actors
        making up the pool."""
        usage_lib.record_library_usage("util.joblib")

        self.ray_remote_args = ray_remote_args
        # Joblib passes Pool-specific backend parameters through ``kwargs``.
        # Consume this one before MultiprocessingBackend forwards the rest.
        self.maxtasksperchild = kwargs.pop("maxtasksperchild", None)
        self.min_size = min_size
        self.max_size = max_size
        self.idle_timeout_s = idle_timeout_s
        super().__init__(
            nesting_level=nesting_level,
            inner_max_num_threads=inner_max_num_threads,
        )
        # Joblib 1.5+ stores these arguments on ParallelBackendBase, while
        # older supported versions forward them to object.__init__ and fail.
        # Own them here so Pool arguments have the same behavior across both.
        self.backend_kwargs = dict(kwargs)

    # ray_remote_args is used both in __init__ and configure to allow for it to be
    # set in both `parallel_backend` and `Parallel` respectively

    def configure(
        self,
        n_jobs: int = 1,
        parallel: Optional[Parallel] = None,
        prefer: Optional[str] = None,
        require: Optional[str] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **memmappingpool_args,
    ):
        """Construct a Ray Pool without mutating Joblib's global pool classes."""
        memmappingpool_args = {**self.backend_kwargs, **memmappingpool_args}
        if self.maxtasksperchild is not None:
            memmappingpool_args.setdefault("maxtasksperchild", self.maxtasksperchild)

        if n_jobs == -1:
            configured_max = self.max_size
            if configured_max is not None:
                n_jobs = configured_max
            elif not ray.is_initialized():
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
            if n_jobs == -1:
                n_jobs = max(int(ray.cluster_resources().get("CPU", 1)), 1)

        eff_n_jobs = self.effective_n_jobs(n_jobs)
        if eff_n_jobs == 1:
            return super().configure(
                1, parallel, prefer, require, **memmappingpool_args
            )

        if any(
            value is not None
            for value in (self.min_size, self.max_size, self.idle_timeout_s)
        ):
            configured_max = self.max_size
            max_size = (
                eff_n_jobs
                if configured_max is None
                else min(configured_max, eff_n_jobs)
            )
            memmappingpool_args["max_size"] = max_size
            if self.min_size is not None:
                memmappingpool_args["min_size"] = min(self.min_size, max_size)
            if self.idle_timeout_s is not None:
                memmappingpool_args["idle_timeout_s"] = self.idle_timeout_s

        pool_args = _configure_pool_args(memmappingpool_args)
        gc.collect()
        self._pool = Pool(
            eff_n_jobs,
            ray_remote_args=(
                ray_remote_args if ray_remote_args is not None else self.ray_remote_args
            ),
            **pool_args,
        )
        self.parallel = parallel
        return eff_n_jobs

    def effective_n_jobs(self, n_jobs):
        eff_n_jobs = super(RayBackend, self).effective_n_jobs(n_jobs)
        if n_jobs == -1 and self.max_size is not None:
            eff_n_jobs = self.max_size
        elif n_jobs == -1 and ray.is_initialized():
            eff_n_jobs = max(int(ray.cluster_resources().get("CPU", 1)), 1)
        return eff_n_jobs


_RAY_POOL_PARAMETERS = inspect.signature(Pool.__init__).parameters
_JOBLIB_MEMMAPPING_PARAMETERS = inspect.signature(MemmappingPool.__init__).parameters


def _configure_pool_args(configure_args: Dict[str, Any]) -> Dict[str, Any]:
    """Select Ray Pool options using the installed Joblib/Ray signatures."""
    pool_args = {}
    for key, value in configure_args.items():
        if key == "context":
            continue
        if key in _RAY_POOL_PARAMETERS and key not in {"self", "processes"}:
            pool_args[key] = value
        elif key not in _JOBLIB_MEMMAPPING_PARAMETERS:
            raise TypeError(f"RayBackend got an unexpected Pool argument: {key}")
    return pool_args
