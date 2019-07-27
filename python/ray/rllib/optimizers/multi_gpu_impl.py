from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import logging

from ray.rllib.utils.debug import log_once, summarize
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

# Variable scope in which created variables will be placed under
TOWER_SCOPE_NAME = "tower"

logger = logging.getLogger(__name__)


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
        input_placeholders: List of input_placeholders for the loss function.
            Tensors of these shapes will be passed to build_graph() in order
            to define the per-device loss ops.
        rnn_inputs: Extra input placeholders for RNN inputs. These will have
            shape [BATCH_SIZE // MAX_SEQ_LEN, ...].
        max_per_device_batch_size: Number of tuples to optimize over at a time
            per device. In each call to `optimize()`,
            `len(devices) * per_device_batch_size` tuples of data will be
            processed. If this is larger than the total data size, it will be
            clipped.
        build_graph: Function that takes the specified inputs and returns a
            TF Policy instance.
    """

    def __init__(self,
                 optimizer,
                 devices,
                 input_placeholders,
                 rnn_inputs,
                 max_per_device_batch_size,
                 build_graph,
                 grad_norm_clipping=None):
        self.optimizer = optimizer
        self.devices = devices
        self.max_per_device_batch_size = max_per_device_batch_size
        self.loss_inputs = input_placeholders + rnn_inputs
        self.build_graph = build_graph

        # First initialize the shared loss network
        with tf.name_scope(TOWER_SCOPE_NAME):
            self._shared_loss = build_graph(self.loss_inputs)
        shared_ops = tf.get_collection(
            tf.GraphKeys.UPDATE_OPS, scope=tf.get_variable_scope().name)

        # Then setup the per-device loss graphs that use the shared weights
        self._batch_index = tf.placeholder(tf.int32, name="batch_index")

        # Dynamic batch size, which may be shrunk if there isn't enough data
        self._per_device_batch_size = tf.placeholder(
            tf.int32, name="per_device_batch_size")
        self._loaded_per_device_batch_size = max_per_device_batch_size

        # When loading RNN input, we dynamically determine the max seq len
        self._max_seq_len = tf.placeholder(tf.int32, name="max_seq_len")
        self._loaded_max_seq_len = 1

        # Split on the CPU in case the data doesn't fit in GPU memory.
        with tf.device("/cpu:0"):
            data_splits = zip(
                *[tf.split(ph, len(devices)) for ph in self.loss_inputs])

        self._towers = []
        for device, device_placeholders in zip(self.devices, data_splits):
            self._towers.append(
                self._setup_device(device, device_placeholders,
                                   len(input_placeholders)))

        avg = average_gradients([t.grads for t in self._towers])
        if grad_norm_clipping:
            clipped = []
            for grad, _ in avg:
                clipped.append(grad)
            clipped, _ = tf.clip_by_global_norm(clipped, grad_norm_clipping)
            for i, (grad, var) in enumerate(avg):
                avg[i] = (clipped[i], var)

        # gather update ops for any batch norm layers. TODO(ekl) here we will
        # use all the ops found which won't work for DQN / DDPG, but those
        # aren't supported with multi-gpu right now anyways.
        self._update_ops = tf.get_collection(
            tf.GraphKeys.UPDATE_OPS, scope=tf.get_variable_scope().name)
        for op in shared_ops:
            self._update_ops.remove(op)  # only care about tower update ops
        if self._update_ops:
            logger.debug("Update ops to run on apply gradient: {}".format(
                self._update_ops))

        with tf.control_dependencies(self._update_ops):
            self._train_op = self.optimizer.apply_gradients(avg)

    def load_data(self, sess, inputs, state_inputs):
        """Bulk loads the specified inputs into device memory.

        The shape of the inputs must conform to the shapes of the input
        placeholders this optimizer was constructed with.

        The data is split equally across all the devices. If the data is not
        evenly divisible by the batch size, excess data will be discarded.

        Args:
            sess: TensorFlow session.
            inputs: List of arrays matching the input placeholders, of shape
                [BATCH_SIZE, ...].
            state_inputs: List of RNN input arrays. These arrays have size
                [BATCH_SIZE / MAX_SEQ_LEN, ...].

        Returns:
            The number of tuples loaded per device.
        """

        if log_once("load_data"):
            logger.info(
                "Training on concatenated sample batches:\n\n{}\n".format(
                    summarize({
                        "placeholders": self.loss_inputs,
                        "inputs": inputs,
                        "state_inputs": state_inputs
                    })))

        feed_dict = {}
        assert len(self.loss_inputs) == len(inputs + state_inputs), \
            (self.loss_inputs, inputs, state_inputs)

        # Let's suppose we have the following input data, and 2 devices:
        # 1 2 3 4 5 6 7                              <- state inputs shape
        # A A A B B B C C C D D D E E E F F F G G G  <- inputs shape
        # The data is truncated and split across devices as follows:
        # |---| seq len = 3
        # |---------------------------------| seq batch size = 6 seqs
        # |----------------| per device batch size = 9 tuples

        if len(state_inputs) > 0:
            smallest_array = state_inputs[0]
            seq_len = len(inputs[0]) // len(state_inputs[0])
            self._loaded_max_seq_len = seq_len
        else:
            smallest_array = inputs[0]
            self._loaded_max_seq_len = 1

        sequences_per_minibatch = (
            self.max_per_device_batch_size // self._loaded_max_seq_len * len(
                self.devices))
        if sequences_per_minibatch < 1:
            logger.warn(
                ("Target minibatch size is {}, however the rollout sequence "
                 "length is {}, hence the minibatch size will be raised to "
                 "{}.").format(self.max_per_device_batch_size,
                               self._loaded_max_seq_len,
                               self._loaded_max_seq_len * len(self.devices)))
            sequences_per_minibatch = 1

        if len(smallest_array) < sequences_per_minibatch:
            # Dynamically shrink the batch size if insufficient data
            sequences_per_minibatch = make_divisible_by(
                len(smallest_array), len(self.devices))

        if log_once("data_slicing"):
            logger.info(
                ("Divided {} rollout sequences, each of length {}, among "
                 "{} devices.").format(
                     len(smallest_array), self._loaded_max_seq_len,
                     len(self.devices)))

        if sequences_per_minibatch < len(self.devices):
            raise ValueError(
                "Must load at least 1 tuple sequence per device. Try "
                "increasing `sgd_minibatch_size` or reducing `max_seq_len` "
                "to ensure that at least one sequence fits per device.")
        self._loaded_per_device_batch_size = (sequences_per_minibatch // len(
            self.devices) * self._loaded_max_seq_len)

        if len(state_inputs) > 0:
            # First truncate the RNN state arrays to the sequences_per_minib.
            state_inputs = [
                make_divisible_by(arr, sequences_per_minibatch)
                for arr in state_inputs
            ]
            # Then truncate the data inputs to match
            inputs = [arr[:len(state_inputs[0]) * seq_len] for arr in inputs]
            assert len(state_inputs[0]) * seq_len == len(inputs[0]), \
                (len(state_inputs[0]), sequences_per_minibatch, seq_len,
                 len(inputs[0]))
            for ph, arr in zip(self.loss_inputs, inputs + state_inputs):
                feed_dict[ph] = arr
            truncated_len = len(inputs[0])
        else:
            for ph, arr in zip(self.loss_inputs, inputs + state_inputs):
                truncated_arr = make_divisible_by(arr, sequences_per_minibatch)
                feed_dict[ph] = truncated_arr
                truncated_len = len(truncated_arr)

        sess.run([t.init_op for t in self._towers], feed_dict=feed_dict)

        self.num_tuples_loaded = truncated_len
        tuples_per_device = truncated_len // len(self.devices)
        assert tuples_per_device > 0, "No data loaded?"
        assert tuples_per_device % self._loaded_per_device_batch_size == 0
        return tuples_per_device

    def optimize(self, sess, batch_index):
        """Run a single step of SGD.

        Runs a SGD step over a slice of the preloaded batch with size given by
        self._loaded_per_device_batch_size and offset given by the batch_index
        argument.

        Updates shared model weights based on the averaged per-device
        gradients.

        Args:
            sess: TensorFlow session.
            batch_index: Offset into the preloaded data. This value must be
                between `0` and `tuples_per_device`. The amount of data to
                process is at most `max_per_device_batch_size`.

        Returns:
            The outputs of extra_ops evaluated over the batch.
        """
        feed_dict = {
            self._batch_index: batch_index,
            self._per_device_batch_size: self._loaded_per_device_batch_size,
            self._max_seq_len: self._loaded_max_seq_len,
        }
        for tower in self._towers:
            feed_dict.update(tower.loss_graph.extra_compute_grad_feed_dict())

        fetches = {"train": self._train_op}
        for tower in self._towers:
            fetches.update(tower.loss_graph._get_grad_and_stats_fetches())

        return sess.run(fetches, feed_dict=feed_dict)

    def get_common_loss(self):
        return self._shared_loss

    def get_device_losses(self):
        return [t.loss_graph for t in self._towers]

    def _setup_device(self, device, device_input_placeholders, num_data_in):
        assert num_data_in <= len(device_input_placeholders)
        with tf.device(device):
            with tf.name_scope(TOWER_SCOPE_NAME):
                device_input_batches = []
                device_input_slices = []
                for i, ph in enumerate(device_input_placeholders):
                    current_batch = tf.Variable(
                        ph,
                        trainable=False,
                        validate_shape=False,
                        collections=[])
                    device_input_batches.append(current_batch)
                    if i < num_data_in:
                        scale = self._max_seq_len
                        granularity = self._max_seq_len
                    else:
                        scale = self._max_seq_len
                        granularity = 1
                    current_slice = tf.slice(
                        current_batch,
                        ([self._batch_index // scale * granularity] +
                         [0] * len(ph.shape[1:])),
                        ([self._per_device_batch_size // scale * granularity] +
                         [-1] * len(ph.shape[1:])))
                    current_slice.set_shape(ph.shape)
                    device_input_slices.append(current_slice)
                graph_obj = self.build_graph(device_input_slices)
                device_grads = graph_obj.gradients(self.optimizer,
                                                   graph_obj._loss)
            return Tower(
                tf.group(
                    *[batch.initializer for batch in device_input_batches]),
                device_grads, graph_obj)


# Each tower is a copy of the loss graph pinned to a specific device.
Tower = namedtuple("Tower", ["init_op", "grads", "loss_graph"])


def make_divisible_by(a, n):
    if type(a) is int:
        return a - a % n
    return a[0:a.shape[0] - a.shape[0] % n]


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
