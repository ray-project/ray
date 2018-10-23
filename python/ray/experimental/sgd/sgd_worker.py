from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import numpy as np
import pyarrow.plasma as plasma
import tensorflow as tf

import ray
from ray.experimental.sgd.sgd_worker import SGDWorker
from ray.experimental.sgd.param_server import ParameterServer
from ray.experimental.sgd.util import Timeline, fetch, run_timeline
from ray.experimental.sgd.modified_allreduce import sum_gradients_all_reduce, \
    unpack_small_tensors

logger = logging.getLogger(__name__)


class SGDWorker(object):
    """Helper class for ray.experimental.sgd.DistributedSGD."""

    def __init__(self,
                 worker_index,
                 model_creator,
                 all_reduce_alg="simple",
                 num_devices=1,
                 gpu=False,
                 max_bytes=10000000,
                 plasma_op=False):
        self.worker_index = worker_index
        assert num_devices > 0

        # TODO(ekl) support custom session
        tf_session_args = {
            "device_count": {
                "CPU": num_devices
            },
            "log_device_placement": False,
            "gpu_options": tf.GPUOptions(force_gpu_compatible=True),
            "inter_op_parallelism_threads": 128,
        }
        config_proto = tf.ConfigProto(**tf_session_args)
        self.sess = tf.Session(config=config_proto)
        self.models = []
        grad_ops = []

        if gpu:
            device_tmpl = "/gpu:%d"
        else:
            device_tmpl = "/cpu:%d"
        for device_idx in range(num_devices):
            device = device_tmpl % device_idx
            with tf.device(device):
                with tf.variable_scope("device_%d" % device_idx):
                    model = model_creator(worker_index, device_idx)
                    self.models.append(model)
                    model.grads = [
                        t
                        for t in model.optimizer.compute_gradients(model.loss)
                        if t[0] is not None
                    ]
                    grad_ops.append(model.grads)

        if num_devices == 1:
            assert not max_bytes, \
                "grad_shard_bytes > 0 ({}) requires num_devices > 1".format(
                    max_bytes)
            self.packed_grads_and_vars = grad_ops
        else:
            if max_bytes:
                self.packed_grads_and_vars, packing_vals = (
                    sum_gradients_all_reduce(
                        "",
                        grad_ops,
                        1,
                        all_reduce_alg,
                        1,
                        list(range(num_devices)),
                        agg_small_grads_max_bytes=max_bytes))
            else:
                self.packed_grads_and_vars, _ = (sum_gradients_all_reduce(
                    "",
                    grad_ops,
                    1,
                    all_reduce_alg,
                    1,
                    list(range(num_devices)),
                    agg_small_grads_max_bytes=0))
        self.per_device_grads = [
            list(zip(*dev_gv))[0] for dev_gv in self.packed_grads_and_vars
        ]
        assert (len(self.per_device_grads) == num_devices)
        self.num_grads = num_grads = len(self.packed_grads_and_vars[0])
        if max_bytes:
            logger.info("Packed grads => {} tensors".format(num_grads))

        # Ops for reading grads with the right control deps
        nccl_noops = []
        for j in range(num_grads)[::-1]:
            deps = nccl_noops + [
                dev_grad[j] for dev_grad in self.per_device_grads
            ]
            with tf.control_dependencies(deps):
                nccl_noops = [tf.no_op()]

        # You must fetch this otherwise the NCCL allreduce will hang
        self.nccl_control_out = tf.group(*nccl_noops)

        round_robin_devices = False
        if plasma_op:
            store_socket = (
                ray.worker.global_worker.plasma_client.store_socket_name)
            manager_socket = (
                ray.worker.global_worker.plasma_client.manager_socket_name)
            if not plasma.tf_plasma_op:
                plasma.build_plasma_tensorflow_op()

            # For fetching grads -> plasma
            self.plasma_in_grads = []
            self.plasma_in_grads_oids = [
                tf.placeholder(shape=[], dtype=tf.string, name="in_grad_oids")
                for _ in range(num_grads)
            ]
            ix = 0
            for j in range(num_grads):
                grad = self.per_device_grads[ix][j]
                if round_robin_devices:
                    ix += 1  # round robin assignment
                ix %= num_devices
                with tf.device(self.models[ix].loss.device):
                    plasma_grad = plasma.tf_plasma_op.tensor_to_plasma(
                        [grad],
                        self.plasma_in_grads_oids[j],
                        plasma_store_socket_name=store_socket,
                        plasma_manager_socket_name=manager_socket)
                self.plasma_in_grads.append(plasma_grad)

            # For applying grads <- plasma
            unpacked_gv = []
            self.plasma_out_grads_oids = [
                tf.placeholder(
                    shape=[], dtype=tf.string, name="grad_out_oids")
                for _ in range(num_grads)
            ]
            packed_plasma_grads = []
            ix = 0
            for j in range(num_grads):
                with tf.device(self.plasma_in_grads[j].device):
                    with tf.control_dependencies([self.plasma_in_grads[j]]):
                        grad_ph = plasma.tf_plasma_op.plasma_to_tensor(
                            self.plasma_out_grads_oids[j],
                            dtype=tf.float32,
                            plasma_store_socket_name=store_socket,
                            plasma_manager_socket_name=manager_socket)
                grad_ph = tf.reshape(grad_ph,
                                     self.packed_grads_and_vars[0][j][0].shape)
                logger.debug("Packed tensor {}".format(grad_ph))
                packed_plasma_grads.append(grad_ph)
            for i in range(num_devices):
                per_device = []
                for j, (g, v) in enumerate(self.packed_grads_and_vars[i]):
                    grad_ph = packed_plasma_grads[j]
                    per_device.append((grad_ph, v))
                unpacked_gv.append(per_device)

            if max_bytes:
                unpacked_gv = unpack_small_tensors(unpacked_gv, packing_vals)

        elif max_bytes:
            unpacked_gv = unpack_small_tensors(self.packed_grads_and_vars,
                                               packing_vals)
        else:
            unpacked_gv = self.packed_grads_and_vars

        # Same shape as packed_grads_and_vars
        assert len(unpacked_gv) == num_devices
        assert len(unpacked_gv[0][0]) == 2

        apply_ops = []
        to_apply = unpacked_gv[0]
        for ix, m in enumerate(self.models):
            apply_ops.append(
                m.optimizer.apply_gradients(
                    [(g, v)
                     for ((g, _), (_, v)) in zip(to_apply, unpacked_gv[ix])]))
        self.apply_op = tf.group(*apply_ops)
        init_op = tf.group(tf.global_variables_initializer(),
                           tf.local_variables_initializer())
        self.sess.run(init_op)

    def foreach_model(self, fn):
        return [fn(m) for m in self.models]

    def foreach_worker(self, fn):
        return fn(self)

    def compute_gradients(self):
        start = time.time()
        feed_dict = {}
        # Aggregate feed dicts for each model on this worker.
        for model in self.models:
            feed_dict.update(model.get_feed_dict())
        # We only need to fetch the first per_device_grad, since they are
        # averaged across all devices by allreduce.
        fetches = self.sess.run(
            [
                self.models[0].loss, self.per_device_grads[0],
                self.nccl_control_out
            ],
            feed_dict=feed_dict)
        logger.debug(
            "compute grad interior time {}".format(time.time() - start))
        return fetches

    def apply_gradients(self, avg_grads):
        start = time.time()
        result = {
            g: avg_grads[i]
            for (i, g) in enumerate(self.per_device_grads[0])
        }
        self.sess.run(self.apply_op, feed_dict=result)
        logger.debug("apply grad interior time {}".format(time.time() - start))

    def ps_compute_apply(self,
                         out_grad_shard_oids,
                         agg_grad_shard_oids,
                         tl_name="ps_compute_apply",
                         write_timeline=False):
        feed_dict = {
            ph: oid
            for (ph,
                 oid) in zip(self.plasma_in_grads_oids, out_grad_shard_oids)
        }
        feed_dict.update({
            ph: oid
            for (ph,
                 oid) in zip(self.plasma_out_grads_oids, agg_grad_shard_oids)
        })
        fetch(agg_grad_shard_oids)
        fetches = run_timeline(
            self.sess, [
                self.models[0].loss, self.plasma_in_grads, self.apply_op,
                self.nccl_control_out
            ],
            feed_dict=feed_dict,
            write_timeline=write_timeline)
        return fetches[0]

    def num_grad_shards(self):
        return self.num_grads

    def shard_shapes(self):
        main_gv = self.packed_grads_and_vars[0]
        return [g.shape for g, _ in main_gv]

    def ip(self):
        return ray.services.get_node_ip_address()
