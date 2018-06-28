from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import os

from tensorflow.python.client import timeline
import tensorflow as tf


# Variable scope in which created variables will be placed under
TOWER_SCOPE_NAME = "tower"


class LocalSyncParallelOptimizer(object):
    """Optimizer that runs in parallel across multiple local devices.

    LocalSyncParallelOptimizer automatically splits up and loads training data
    onto specified local devices (e.g. GPUs) with `load_data()`. During a call
    to `optimize()`, the devices compute gradients over slices of the data in
    parallel. The gradients are then averaged and applied to the shared
    weights.

    The data loaded is pinned in device memory until the next call to
    `load_data`, so you can make multiple passes (possibly in randomized order)
    over the same data once loaded.

    This is similar to tf.train.SyncReplicasOptimizer, but works within a
    single TensorFlow graph, i.e. implements in-graph replicated training:

      https://www.tensorflow.org/api_docs/python/tf/train/SyncReplicasOptimizer

    Args:
        optimizer: Delegate TensorFlow optimizer object.
        devices: List of the names of TensorFlow devices to parallelize over.
        input_placeholders: List of (name, input_placeholder)
            for the loss function. Tensors of these shapes will be passed
            to build_graph() in order to define the per-device loss ops.
        per_device_batch_size: Number of tuples to optimize over at a time per
            device. In each call to `optimize()`,
            `len(devices) * per_device_batch_size` tuples of data will be
            processed.
        build_graph: Function that takes the specified inputs and returns a
            TF Policy Graph instance.
        logdir: Directory to place debugging output in.
        grad_norm_clipping: None or int stdev to clip grad norms by
    """

    def __init__(self, optimizer, devices, input_placeholders,
                 per_device_batch_size, build_graph, logdir,
                 grad_norm_clipping=None):
        # TODO(rliaw): remove logdir
        self.optimizer = optimizer
        self.devices = devices
        self.batch_size = per_device_batch_size * len(devices)
        self.per_device_batch_size = per_device_batch_size
        self.loss_inputs = input_placeholders
        self.build_graph = build_graph
        self.logdir = logdir

        # First initialize the shared loss network
        with tf.name_scope(TOWER_SCOPE_NAME):
            self._shared_loss = build_graph(input_placeholders)

        # Then setup the per-device loss graphs that use the shared weights
        self._batch_index = tf.placeholder(tf.int32)

        # Split on the CPU in case the data doesn't fit in GPU memory.
        with tf.device("/cpu:0"):
            names, placeholders = zip(*input_placeholders)
            data_splits = zip(
                *[tf.split(ph, len(devices)) for ph in placeholders])

        self._towers = []
        for device, device_placeholders in zip(self.devices, data_splits):
            self._towers.append(
                self._setup_device(device, zip(names, device_placeholders)))

        avg = average_gradients([t.grads for t in self._towers])
        if grad_norm_clipping:
            for i, (grad, var) in enumerate(avg):
                if grad is not None:
                    avg[i] = (tf.clip_by_norm(grad, grad_norm_clipping), var)
        self._train_op = self.optimizer.apply_gradients(avg)

    def load_data(self, sess, inputs, full_trace=False):
        """Bulk loads the specified inputs into device memory.

        The shape of the inputs must conform to the shapes of the input
        placeholders this optimizer was constructed with.

        The data is split equally across all the devices. If the data is not
        evenly divisible by the batch size, excess data will be discarded.

        Args:
            sess: TensorFlow session.
            inputs: List of Tensors matching the input placeholders specified
                at construction time of this optimizer.
            full_trace: Whether to profile data loading.

        Returns:
            The number of tuples loaded per device.
        """

        feed_dict = {}
        assert len(self.loss_inputs) == len(inputs)
        for (name, ph), arr in zip(self.loss_inputs, inputs):
            truncated_arr = make_divisible_by(arr, self.batch_size)
            feed_dict[ph] = truncated_arr
            truncated_len = len(truncated_arr)

        if full_trace:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
        else:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
        run_metadata = tf.RunMetadata()

        sess.run(
            [t.init_op for t in self._towers],
            feed_dict=feed_dict,
            options=run_options,
            run_metadata=run_metadata)
        if full_trace:
            trace = timeline.Timeline(step_stats=run_metadata.step_stats)
            trace_file = open(os.path.join(self.logdir, "timeline-load.json"),
                              "w")
            trace_file.write(trace.generate_chrome_trace_format())

        tuples_per_device = truncated_len / len(self.devices)
        assert tuples_per_device > 0, \
            "Too few tuples per batch, trying increasing the training " \
            "batch size or decreasing the sgd batch size. Tried to split up " \
            "{} rows {}-ways in batches of {} (total across devices).".format(
                len(arr), len(self.devices), self.batch_size)
        assert tuples_per_device % self.per_device_batch_size == 0
        return tuples_per_device

    def optimize(self, sess, batch_index, file_writer=None):
        """Run a single step of SGD.

        Runs a SGD step over a slice of the preloaded batch with size given by
        self.per_device_batch_size and offset given by the batch_index
        argument.

        Updates shared model weights based on the averaged per-device
        gradients.

        Args:
            sess: TensorFlow session.
            batch_index: Offset into the preloaded data. This value must be
                between `0` and `tuples_per_device`. The amount of data to
                process is always fixed to `per_device_batch_size`.
            file_writer: If specified, tf metrics will be written out using
                this.

        Returns:
            The outputs of extra_ops evaluated over the batch.
        """
        if file_writer:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
        else:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
        run_metadata = tf.RunMetadata()

        feed_dict = {self._batch_index: batch_index}
        for tower in self._towers:
            feed_dict.update(tower.loss_graph.extra_compute_grad_feed_dict())
            feed_dict.update(tower.loss_graph.extra_apply_grad_feed_dict())

        fetches = {"train": self._train_op}
        for tower in self._towers:
            fetches.update(tower.loss_graph.extra_compute_grad_fetches())
            fetches.update(tower.loss_graph.extra_apply_grad_fetches())

        outs = sess.run(
            fetches,
            feed_dict=feed_dict,
            options=run_options,
            run_metadata=run_metadata)

        if file_writer:
            trace = timeline.Timeline(step_stats=run_metadata.step_stats)
            trace_file = open(os.path.join(self.logdir, "timeline-sgd.json"),
                              "w")
            trace_file.write(trace.generate_chrome_trace_format())
            file_writer.add_run_metadata(
                run_metadata, "sgd_train_{}".format(batch_index))

        return outs

    def get_common_loss(self):
        return self._shared_loss

    def get_device_losses(self):
        return [t.loss_graph for t in self._towers]

    def _setup_device(self, device, device_input_placeholders):
        with tf.device(device):
            with tf.name_scope(TOWER_SCOPE_NAME):
                device_input_batches = []
                device_input_slices = []
                for name, ph in device_input_placeholders:
                    current_batch = tf.Variable(
                        ph, trainable=False, validate_shape=False,
                        collections=[])
                    device_input_batches.append(current_batch)
                    current_slice = tf.slice(
                        current_batch,
                        [self._batch_index] + [0] * len(ph.shape[1:]),
                        ([self.per_device_batch_size] + [-1] *
                         len(ph.shape[1:])))
                    current_slice.set_shape(ph.shape)
                    device_input_slices.append((name, current_slice))
                graph_obj = self.build_graph(device_input_slices)
                device_grads = graph_obj.gradients(self.optimizer)
            return Tower(
                tf.group(*[batch.initializer
                           for batch in device_input_batches]),
                device_grads,
                graph_obj)


# Each tower is a copy of the loss graph pinned to a specific device.
Tower = namedtuple("Tower", ["init_op", "grads", "loss_graph"])


def make_divisible_by(array, n):
    return array[0:array.shape[0] - array.shape[0] % n]


def average_gradients(tower_grads):
    """Averages gradients across towers.

    Calculate the average gradient for each shared variable across all towers.
    Note that this function provides a synchronization point across all towers.

    Args:
        tower_grads: List of lists of (gradient, variable) tuples. The outer
            list is over individual gradients. The inner list is over the
            gradient calculation for each tower.

    Returns:
       List of pairs of (gradient, variable) where the gradient has been
           averaged across all towers.

    TODO(ekl): We could use NCCL if this becomes a bottleneck.
    """

    average_grads = []
    for grad_and_vars in zip(*tower_grads):

        # Note that each grad_and_vars looks like the following:
        #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
        grads = []
        for g, _ in grad_and_vars:
            if g is not None:
                # Add 0 dimension to the gradients to represent the tower.
                expanded_g = tf.expand_dims(g, 0)

                # Append on a 'tower' dimension which we will average over
                # below.
                grads.append(expanded_g)

        if not grads:
            continue

        # Average over the 'tower' dimension.
        grad = tf.concat(axis=0, values=grads)
        grad = tf.reduce_mean(grad, 0)

        # Keep in mind that the Variables are redundant because they are shared
        # across towers. So .. we will just return the first tower's pointer to
        # the Variable.
        v = grad_and_vars[0][1]
        grad_and_var = (grad, v)
        average_grads.append(grad_and_var)

    return average_grads
