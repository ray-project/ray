from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import ray
import time

from tensorflow.python.client import timeline
import numpy as np
import tensorflow as tf
import tensorflow.contrib.nccl as nccl
import tensorflow.contrib.slim as slim

from util import Timeline, fetch, run_timeline
from tfbench import modified_allreduce


class SGDWorker(object):
    def __init__(self,
                 worker_index,
                 model_creator,
                 all_reduce_alg="simple",
                 num_devices=1,
                 use_cpus=False,
                 max_bytes=0,
                 plasma_op=False,
                 verbose=False):
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

        if use_cpus:
            device_tmpl = "/cpu:%d"
        else:
            device_tmpl = "/gpu:%d"
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
            assert not max_bytes, "Not supported with 1 GPU"
            self.packed_grads_and_vars = grad_ops
        else:
            if max_bytes:
                self.packed_grads_and_vars, packing_vals = (
                    modified_allreduce.sum_gradients_all_reduce(
                        "",
                        grad_ops,
                        1,
                        all_reduce_alg,
                        1,
                        list(range(num_devices)),
                        agg_small_grads_max_bytes=max_bytes))
            else:
                self.packed_grads_and_vars, _ = (
                    modified_allreduce.sum_gradients_all_reduce(
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
            print("Packed grads => {} tensors".format(num_grads))

        # Ops for reading grads with the right control deps
        nccl_noops = []
        for j in range(num_grads)[::-1]:
            with tf.control_dependencies(
                    nccl_noops +
                [dev_grad[j] for dev_grad in self.per_device_grads]):
                nccl_noops = [tf.no_op()]

        # You must fetch this otherwise the NCCL allreduce will hang
        self.nccl_control_out = tf.group(*nccl_noops)

        round_robin_devices = False
        if plasma_op:
            store_socket = (
                ray.worker.global_worker.plasma_client.store_socket_name)
            manager_socket = (
                ray.worker.global_worker.plasma_client.manager_socket_name)
            memcpy_plasma_module = tf.load_op_library(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "ops/memcpy_plasma_op.so"))

            # For fetching grads -> plasma
            self.plasma_in_grads = []
            self.plasma_in_grads_oids = [
                tf.placeholder(shape=[], dtype=tf.string)
                for _ in range(num_grads)
            ]
            ix = 0
            for j in range(num_grads):
                grad = self.per_device_grads[ix][j]
                if round_robin_devices:
                    ix += 1  # round robin assignment
                ix %= num_devices
                with tf.device(self.models[ix].device):
                    plasma_grad = memcpy_plasma_module.tensor_to_plasma(
                        [grad],
                        self.plasma_in_grads_oids[j],
                        plasma_store_socket_name=store_socket,
                        plasma_manager_socket_name=manager_socket)
                self.plasma_in_grads.append(plasma_grad)

            # For applying grads <- plasma
            unpacked_gv = []
            self.plasma_out_grads_oids = [
                tf.placeholder(shape=[], dtype=tf.string)
                for _ in range(num_grads)
            ]
            packed_plasma_grads = []
            ix = 0
            for j in range(num_grads):
                with tf.device(self.plasma_in_grads[j].device):
                    with tf.control_dependencies([self.plasma_in_grads[j]]):
                        grad_ph = memcpy_plasma_module.plasma_to_tensor(
                            self.plasma_out_grads_oids[j],
                            plasma_store_socket_name=store_socket,
                            plasma_manager_socket_name=manager_socket)
                grad_ph = tf.reshape(grad_ph,
                                     self.packed_grads_and_vars[0][j][0].shape)
                print("Packed tensor", grad_ph)
                packed_plasma_grads.append(grad_ph)
            for i in range(num_devices):
                per_device = []
                for j, (g, v) in enumerate(self.packed_grads_and_vars[i]):
                    grad_ph = packed_plasma_grads[j]
                    per_device.append((grad_ph, v))
                unpacked_gv.append(per_device)

            if max_bytes:
                unpacked_gv = allreduce.unpack_small_tensors(
                    unpacked_gv, packing_vals)

        elif max_bytes:
            unpacked_gv = allreduce.unpack_small_tensors(
                self.packed_grads_and_vars, packing_vals)
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

    def compute_gradients(self, verbose):
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
        if verbose:
            print("compute grad interior time", time.time() - start)
        return fetches

    def apply_gradients(self, avg_grads, verbose):
        start = time.time()
        result = {
            g: avg_grads[i]
            for (i, g) in enumerate(self.per_device_grads[0])
        }
        self.sess.run(self.apply_op, feed_dict=result)
        if verbose:
            print("apply grad interior time", time.time() - start)

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
        run_timeline(
            self.sess,
            [self.plasma_in_grads, self.apply_op, self.nccl_control_out],
            feed_dict=feed_dict,
            write_timeline=write_timeline)

    def num_grad_shards(self):
        return self.num_grads

    def shard_shapes(self):
        main_gv = self.packed_grads_and_vars[0]
        return [g.shape for g, _ in main_gv]

    def ip(self):
        return ray.services.get_node_ip_address()


class ParameterServer(object):
    def __init__(self, num_workers, tid):
        self.num_sgd_workers = num_workers
        self.acc_counter = 0
        self.timeline = Timeline(tid)
        self.timeline.patch_ray()

    def set_tid(self, tid):
        self.timeline.tid = tid

    def get_time(self):
        return time.time() + self.timeline.offset

    def set_time(self, ref_time):
        self.timeline.offset = ref_time - time.time()

    def initialize(self, shard_shape):
        self.accumulated = np.zeros(shard_shape, dtype=np.float32)

    def mark(self):
        self.timeline.event("mark")

    def prefetch(self, oids):
        self.timeline.reset()
        self.timeline.start("prefetch")
        fetch(oids)
        self.timeline.end("prefetch")

    def add_spinwait(self, grad_shard_ids):
        self.timeline.start("add_spinwait")
        plasma_ids = [ray.pyarrow.plasma.ObjectID(x) for x in grad_shard_ids]
        while plasma_ids:
            for p in plasma_ids:
                if ray.worker.global_worker.plasma_client.contains(p):
                    self.timeline.start("get_buffers")
                    [raw_grads
                     ] = (ray.worker.global_worker.plasma_client.get_buffers(
                         [p]))
                    grads = np.frombuffer(raw_grads, dtype=np.float32)
                    self.accumulated += grads
                    self.acc_counter += 1
                    self.timeline.end("get_buffers")
                    plasma_ids.remove(p)
                    break
        self.timeline.end("add_spinwait")

    def add(self, grad_shard_id):
        self.timeline.start("add")
        # self.timeline.start("add_wait")
        # ray.wait([ray.local_scheduler.ObjectID(grad_shard_id)])
        # self.timeline.end("add_wait")
        self.timeline.start("get_buffers")
        oid = ray.pyarrow.plasma.ObjectID(grad_shard_id)
        [raw_grads] = ray.worker.global_worker.plasma_client.get_buffers([oid])
        grads = np.frombuffer(raw_grads, dtype=np.float32)
        self.timeline.end("get_buffers")
        self.accumulated += grads
        self.acc_counter += 1
        self.timeline.end("add")

    def get(self, object_id):
        self.timeline.start("get")
        client = ray.worker.global_worker.plasma_client
        assert self.acc_counter == self.num_sgd_workers, self.acc_counter
        oid = ray.pyarrow.plasma.ObjectID(object_id)
        buff = client.create(oid, self.accumulated.nbytes)
        wrapper = np.frombuffer(buff, dtype=np.float32)
        np.copyto(wrapper, self.accumulated)
        client.seal(oid)
        self.accumulated = np.zeros_like(self.accumulated)
        self.acc_counter = 0
        self.timeline.end("get")

    def get_timeline(self):
        return self.timeline

    def ip(self):
        return ray.services.get_node_ip_address()

    def pin(self, cpu_id):
        try:
            import psutil
            p = psutil.Process()
            p.cpu_affinity([cpu_id])
            print("Setting CPU Affinity to: ", cpu_id)
        except Exception as e:
            print(e)


def average_gradients(grads):
    out = []
    for grad_list in zip(*grads):
        out.append(np.mean(grad_list, axis=0))
    return out


def do_sgd_step(actors, verbose):
    start = time.time()
    fetches = ray.get([a.compute_gradients.remote(verbose) for a in actors])
    losses = [f[0] for f in fetches]
    grads = [f[1] for f in fetches]
    if verbose:
        print("compute all grads time", time.time() - start)
    start = time.time()
    if len(actors) == 1:
        assert len(grads) == 1
        avg_grad = grads[0]
    else:
        avg_grad = average_gradients(grads)
        if verbose:
            print("grad reduce time", time.time() - start)
    start = time.time()
    ray.get([a.apply_gradients.remote(avg_grad, verbose) for a in actors])
    if verbose:
        print("apply all grads time", time.time() - start)
    return np.mean(losses)


def distributed_sgd_step(actors, ps_list, verbose, write_timeline):
    # Preallocate object ids that actors will write gradient shards to
    grad_shard_oids_list = [[np.random.bytes(20) for _ in ps_list]
                            for _ in actors]
    print("generated grad oids")

    # Preallocate object ids that param servers will write new weights to
    accum_shard_ids = [np.random.bytes(20) for _ in ps_list]
    print("generated accum oids")

    # Kick off the fused compute grad / update weights tf run for each actor
    for actor, grad_shard_oids in zip(actors, grad_shard_oids_list):
        actor.ps_compute_apply.remote(
            grad_shard_oids, accum_shard_ids, write_timeline=write_timeline)
    print("Launched all ps_compute_applys on all actors")

    # Issue prefetch ops
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        to_fetch = []
        for grad_shard_oids in grad_shard_oids_list:
            to_fetch.append(grad_shard_oids[j])
        random.shuffle(to_fetch)
        ps.prefetch.remote(to_fetch)
    print("Launched all prefetch ops")

    # Aggregate the gradients produced by the actors. These operations
    # run concurrently with the actor methods above.
    ps_gets = []
    for j, (ps, weight_shard_oid) in list(
            enumerate(zip(ps_list, accum_shard_ids)))[::-1]:
        ps.add_spinwait.remote([gs[j] for gs in grad_shard_oids_list])
        ps_gets.append(ps.get.remote(weight_shard_oid))
    print("Launched all aggregate ops")

    if verbose:
        timelines = [ps.get_timeline.remote() for ps in ps_list]
        print("launched timeline gets")
        timelines = ray.get(timelines)
        t0 = timelines[0]
        for t in timelines[1:]:
            t0.merge(t)
        t0.chrome_trace_format("ps_timeline.json")
    else:
        # Wait for at least the ps gets to finish
        ray.get(ps_gets)


def roundrobin_ps(ps_cls, sgd_workers, shard_shapes, spread_ps):
    worker_ips = ray.get([w.ip.remote() for w in sgd_workers])
    num_ips = len(set(worker_ips))
    num_workers = len(sgd_workers)
    min_placed = np.ceil(len(shard_shapes) / num_ips)
    from collections import Counter, defaultdict
    tid_counter = [0]

    def create_ps():
        tid_counter[0] += 1
        return RemotePS.remote(num_workers, tid_counter[0])

    ip_mapping = defaultdict(list)

    while (any(len(v) < min_placed for v in ip_mapping.values())
           or (len(ip_mapping) < num_ips)):
        print("generating new ps, ip map so far", ip_mapping)
        new_ps = create_ps()
        ps_ip = ray.get(new_ps.ip.remote())
        if spread_ps and ps_ip in worker_ips:
            print("ignoring ps that is on same node as worker")
        elif not spread_ps and ps_ip not in worker_ips:
            print("ignoring ps that NOT on same node as some worker")
        else:
            ip_mapping[ps_ip] += [new_ps]

    final_list = []
    candidates = list(ip_mapping.values())
    for i, s in enumerate(shard_shapes):
        ps = candidates[i % num_ips][i // num_ips]
        final_list += [ps]
        ps.initialize.remote(s)

    for ps in sum(candidates, []):
        if ps not in final_list:
            ps.__ray_terminate__.remote(ps._ray_actor_id.id())
            print("removing a ps...")
        else:
            print("saving ps...")

    print("Final PS balance: ",
          Counter(ray.get([ps.ip.remote() for ps in final_list])))
    for i, ps in enumerate(final_list):
        ps.set_tid.remote(i)
    return final_list


class DistributedSGD(object):
    def __init__(self, model_creator, num_workers, devices_per_worker,
                 use_cpus):
        self.model_creator = model_creator
        if use_cpus:
            requests = {"num_cpus": devices_per_worker}
        else:
            requests = {"num_gpus": devices_per_worker}
        RemoteSGDWorker = ray.remote(**requests)(SGDWorker)
        self.workers = []
        for worker_index in range(num_workers):
            print("Creating worker", worker_index)
            self.workers.append(
                RemoteSGDWorker.remote(
                    worker_index,
                    model_creator,
                    num_devices=devices_per_worker,
                    use_cpus=use_cpus,
                    verbose=True))

    def foreach_worker(self, fn):
        results = ray.get([w.foreach_worker.remote(fn) for w in self.workers])
        return results

    def foreach_model(self, fn):
        results = ray.get([w.foreach_model.remote(fn) for w in self.workers])
        out = []
        for r in results:
            out.extend(r)
        return r

    def step(self):
        return do_sgd_step(self.workers, True)
