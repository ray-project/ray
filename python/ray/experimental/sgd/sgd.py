from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
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
        if strategy == "ps":
            use_plasma_op = True
        elif strategy == "simple":
            use_plasma_op = False
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
        logger.info("Creating SGD workers ({} total)".format(num_workers))
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
        results = ray.get([w.foreach_worker.remote(fn) for w in self.workers])
        return results

    def foreach_model(self, fn):
        results = ray.get([w.foreach_model.remote(fn) for w in self.workers])
        out = []
        for r in results:
            out.extend(r)
        return r

    def warmup(self):
        logger.info("Warming up object store of worker actors")
        ray.get([w.warmup.remote() for w in self.workers])
        logger.info("Warmup complete")

    def step(self, fetch_stats=False):
        if self.strategy == "ps":
            return _distributed_sgd_step(
                self.workers,
                self.ps_list,
                write_timeline=False,
                fetch_stats=fetch_stats)
        else:
            return _simple_sgd_step(self.workers)


def _average_gradients(grads):
    out = []
    for grad_list in zip(*grads):
        out.append(np.mean(grad_list, axis=0))
    return out


def _simple_sgd_step(actors):
    if len(actors) == 1:
        return ray.get(actors[0].compute_apply.remote())

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
    return np.mean(losses)


def _distributed_sgd_step(actors, ps_list, fetch_stats, write_timeline):
    # Preallocate object ids that actors will write gradient shards to
    grad_shard_oids_list = [[np.random.bytes(20) for _ in ps_list]
                            for _ in actors]
    logger.info("Generated grad oids")

    # Preallocate object ids that param servers will write new weights to
    accum_shard_ids = [np.random.bytes(20) for _ in ps_list]
    logger.info("Generated accum oids")

    # Kick off the fused compute grad / update weights tf run for each actor
    losses = []
    for actor, grad_shard_oids in zip(actors, grad_shard_oids_list):
        losses.append(
            actor.ps_compute_apply.remote(
                grad_shard_oids,
                accum_shard_ids,
                write_timeline=write_timeline))
    logger.info("Launched all ps_compute_applys on all actors")

    # Issue prefetch ops
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        to_fetch = []
        for grad_shard_oids in grad_shard_oids_list:
            to_fetch.append(grad_shard_oids[j])
        random.shuffle(to_fetch)
        ps.prefetch.remote(to_fetch)
    logger.info("Launched all prefetch ops")

    # Aggregate the gradients produced by the actors. These operations
    # run concurrently with the actor methods above.
    ps_gets = []
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        ps.add_spinwait.remote([gs[j] for gs in grad_shard_oids_list])
        ps_gets.append(ps.get.remote(weight_shard_oid))
    logger.info("Launched all aggregate ops")

    if write_timeline:
        timelines = [ps.get_timeline.remote() for ps in ps_list]
        logger.info("launched timeline gets")
        timelines = ray.get(timelines)
        t0 = timelines[0]
        for t in timelines[1:]:
            t0.merge(t)
        t0.chrome_trace_format("ps_timeline.json")
    else:
        # Wait for at least the ps gets to finish
        ray.get(ps_gets)
    if fetch_stats:
        return np.mean(ray.get(losses))
    else:
        return None
