from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import random
import time

import numpy as np

import ray
from ray.experimental.sgd.sgd_worker import SGDWorker
from ray.experimental.sgd.param_server import ParameterServer

logger = logging.getLogger(__name__)


class DistributedSGD(object):
    """Experimental distributed SGD implementation in Ray.

    This supports two modes:
        'simple': centralized gradient aggregation
        'ps': sharded parameter-server implementation

    To use this class, you'll have to implement model.py:Model.

    Arguments:
        model_creator (func): Function that returns a model given worker and
            device indexes as arguments. Each model replica will be created
            within its own variable scope.
        num_workers (int): Number of Ray actors to use for SGD.
        devices_per_worker (int): Number of GPU or CPU devices to use per
            worker. One model replica will be created per device.
        gpu (bool): Whether to use GPU devices.
        strategy (str): Strategy to use for distributed gradient aggregation.
            This only applies if num_workers > 1.
        grad_shard_bytes (int): Fuse gradient tensors into chunks of at most
            this size (if applicable).
        all_reduce_alg (str): TensorFlow strategy to use for gradient
            synchronization within the same worker (if applicable).
            See modified_allreduce.py for options.

    Examples:
        >>> # Setup distributed SGD
        >>> model_creator = (
        ...   lambda worker_idx, device_idx: YourModelClass(...))
        >>> sgd = DistributedSGD(
        ...   model_creator, num_workers=2,
        ...   devices_per_worker=4, gpu=True, strategy="ps")

        >>> # To train
        >>> for i in range(100):
        ...   stats = sgd.step(fetch_stats=i % 10 == 0)

        >>> # To access or update model state
        >>> sgd.foreach_model(lambda model: ...)

        >>> # To access or update worker state
        >>> sgd.foreach_worker(lambda worker: ...)
    """

    def __init__(self,
                 model_creator,
                 num_workers,
                 devices_per_worker,
                 gpu=True,
                 strategy="ps",
                 grad_shard_bytes=10000000,
                 all_reduce_alg="simple"):

        if num_workers == 1 and strategy == "ps":
            logger.warning(
                "The parameter server strategy does not make sense for single "
                "worker operation, falling back to simple mode.")
            strategy = "simple"

        if strategy == "ps":
            use_plasma_op = True
        elif strategy == "simple":
            use_plasma_op = False
            grad_shard_bytes = 0  # tensor fusion doesn't make sense
        else:
            raise ValueError("strategy must be one of 'ps', 'simple'")
        self.strategy = strategy

        self.model_creator = model_creator
        if gpu:
            requests = {"num_gpus": devices_per_worker}
        else:
            requests = {"num_cpus": devices_per_worker}

        RemoteSGDWorker = ray.remote(**requests)(SGDWorker)
        self.workers = []
        logger.info(
            "Creating SGD workers ({} total, {} devices per worker)".format(
                num_workers, devices_per_worker))
        for worker_index in range(num_workers):
            self.workers.append(
                RemoteSGDWorker.remote(
                    worker_index,
                    model_creator,
                    num_devices=devices_per_worker,
                    plasma_op=use_plasma_op,
                    gpu=gpu,
                    max_bytes=grad_shard_bytes,
                    all_reduce_alg=all_reduce_alg))

        logger.info("Waiting for gradient configuration")
        shard_shapes = ray.get(self.workers[0].shard_shapes.remote())

        logger.info("Waiting for actors to start")
        ray.get([w.shard_shapes.remote() for w in self.workers])

        if strategy == "ps":
            logger.info("Starting parameter servers ({} shards)".format(
                len(shard_shapes)))
            self.ps_list = [
                ParameterServer.remote(len(self.workers), i)
                for i, s in enumerate(shard_shapes)
            ]
            ray.get([
                ps.initialize.remote(s)
                for ps, s in zip(self.ps_list, shard_shapes)
            ])
            logger.info("Parameter servers started")
        else:
            self.ps_list = []

    def foreach_worker(self, fn):
        """Apply the given function to each remote worker.

        Returns:
            List of results from applying the function.
        """
        results = ray.get([w.foreach_worker.remote(fn) for w in self.workers])
        return results

    def foreach_model(self, fn):
        """Apply the given function to each model replica in each worker.

        Returns:
            List of results from applying the function.
        """

        results = ray.get([w.foreach_model.remote(fn) for w in self.workers])
        out = []
        for r in results:
            out.extend(r)
        return out

    def for_model(self, fn):
        """Apply the given function to a single model replica.

        Returns:
            Result from applying the function.
        """
        return ray.get(self.workers[0].for_model.remote(fn))

    def step(self, fetch_stats=False):
        """Run a single SGD step.

        Arguments:
            fetch_stats (bool): Whether to return stats from the step. This can
                slow down the computation by acting as a global barrier.
        """
        if self.strategy == "ps":
            return _distributed_sgd_step(
                self.workers,
                self.ps_list,
                write_timeline=False,
                fetch_stats=fetch_stats)
        else:
            return _simple_sgd_step(self.workers)

    def warmup(self):
        logger.info("Warming up object store of worker actors")
        ray.get([w.warmup.remote() for w in self.workers])
        logger.info("Warmup complete")

    def save_checkpoint(self, path):
        w0 = self.for_model(lambda m: m.get_weights())
        filename = os.path.join(path, "model.npy")
        np.save(filename, w0)

    def restore_checkpoint(self, path):
        filename = os.path.join(path, "model.npy")
        w0 = np.load(filename)
        self.foreach_model(lambda m: m.set_weights(w0))


def _average_gradients(grads):
    out = []
    for grad_list in zip(*grads):
        out.append(np.mean(grad_list, axis=0))
    return out


def _simple_sgd_step(actors):
    if len(actors) == 1:
        return {"loss": ray.get(actors[0].compute_apply.remote())}

    start = time.time()
    fetches = ray.get([a.compute_gradients.remote() for a in actors])
    losses = [f[0] for f in fetches]
    grads = [f[1] for f in fetches]
    logger.debug("compute all grads time {}".format(time.time() - start))
    start = time.time()
    if len(actors) == 1:
        assert len(grads) == 1
        avg_grad = grads[0]
    else:
        avg_grad = _average_gradients(grads)
        logger.debug("grad reduce time {}".format(time.time() - start))
    start = time.time()
    ray.get([a.apply_gradients.remote(avg_grad) for a in actors])
    logger.debug("apply all grads time {}".format(time.time() - start))
    return {"loss": np.mean(losses)}


def _distributed_sgd_step(actors, ps_list, fetch_stats, write_timeline):
    # Preallocate object ids that actors will write gradient shards to
    grad_shard_oids_list = [[np.random.bytes(20) for _ in ps_list]
                            for _ in actors]
    logger.debug("Generated grad oids")

    # Preallocate object ids that param servers will write new weights to
    accum_shard_ids = [np.random.bytes(20) for _ in ps_list]
    logger.debug("Generated accum oids")

    # Kick off the fused compute grad / update weights tf run for each actor
    losses = []
    for actor, grad_shard_oids in zip(actors, grad_shard_oids_list):
        losses.append(
            actor.ps_compute_apply.remote(
                grad_shard_oids,
                accum_shard_ids,
                write_timeline=write_timeline))
    logger.debug("Launched all ps_compute_applys on all actors")

    # Issue prefetch ops
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        to_fetch = []
        for grad_shard_oids in grad_shard_oids_list:
            to_fetch.append(grad_shard_oids[j])
        random.shuffle(to_fetch)
        ps.prefetch.remote(to_fetch)
    logger.debug("Launched all prefetch ops")

    # Aggregate the gradients produced by the actors. These operations
    # run concurrently with the actor methods above.
    ps_gets = []
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        ps.add_spinwait.remote([gs[j] for gs in grad_shard_oids_list])
        ps_gets.append(ps.get.remote(weight_shard_oid))
    logger.debug("Launched all aggregate ops")

    if write_timeline:
        timelines = [ps.get_timeline.remote() for ps in ps_list]
        logger.debug("Launched timeline gets")
        timelines = ray.get(timelines)
        t0 = timelines[0]
        for t in timelines[1:]:
            t0.merge(t)
        t0.chrome_trace_format("ps_timeline.json")
    else:
        # Wait for at least the ps gets to finish
        ray.get(ps_gets)
    if fetch_stats:
        return {"loss": np.mean(ray.get(losses))}
    else:
        return None
