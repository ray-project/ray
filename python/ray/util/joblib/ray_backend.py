from joblib._parallel_backends import MultiprocessingBackend
from joblib.pool import PicklingPool
import logging

from ray.util.multiprocessing.pool import Pool
import ray

logger = logging.getLogger(__name__)


class RayBackend(MultiprocessingBackend):
    """Ray backend uses ray, a system for scalable distributed computing.
    More info about Ray is available here: https://docs.ray.io.
    """

    def configure(
        self, n_jobs=1, parallel=None, prefer=None, require=None, **memmappingpool_args
    ):
        """Make Ray Pool the father class of PicklingPool. PicklingPool is a
        father class that inherits Pool from multiprocessing.pool. The next
        line is a patch, which changes the inheritance of Pool to be from
        ray.util.multiprocessing.pool.
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
            ray_cpus = int(ray.state.cluster_resources()["CPU"])
            n_jobs = ray_cpus

        eff_n_jobs = super(RayBackend, self).configure(
            n_jobs, parallel, prefer, require, **memmappingpool_args
        )
        return eff_n_jobs

    def effective_n_jobs(self, n_jobs):
        eff_n_jobs = super(RayBackend, self).effective_n_jobs(n_jobs)
        if n_jobs == -1:
            ray_cpus = int(ray.state.cluster_resources()["CPU"])
            eff_n_jobs = ray_cpus
        return eff_n_jobs
