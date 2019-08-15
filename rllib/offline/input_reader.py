from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import threading

from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)


@PublicAPI
class InputReader(object):
    """Input object for loading experiences in policy evaluation."""

    @PublicAPI
    def next(self):
        """Return the next batch of experiences read.

        Returns:
            SampleBatch or MultiAgentBatch read.
        """
        raise NotImplementedError

    @PublicAPI
    def tf_input_ops(self, queue_size=1):
        """Returns TensorFlow queue ops for reading inputs from this reader.

        The main use of these ops is for integration into custom model losses.
        For example, you can use tf_input_ops() to read from files of external
        experiences to add an imitation learning loss to your model.

        This method creates a queue runner thread that will call next() on this
        reader repeatedly to feed the TensorFlow queue.

        Arguments:
            queue_size (int): Max elements to allow in the TF queue.

        Example:
            >>> class MyModel(rllib.model.Model):
            ...     def custom_loss(self, policy_loss, loss_inputs):
            ...         reader = JsonReader(...)
            ...         input_ops = reader.tf_input_ops()
            ...         logits, _ = self._build_layers_v2(
            ...             {"obs": input_ops["obs"]},
            ...             self.num_outputs, self.options)
            ...         il_loss = imitation_loss(logits, input_ops["action"])
            ...         return policy_loss + il_loss

        You can find a runnable version of this in examples/custom_loss.py.

        Returns:
            dict of Tensors, one for each column of the read SampleBatch.
        """

        if hasattr(self, "_queue_runner"):
            raise ValueError(
                "A queue runner already exists for this input reader. "
                "You can only call tf_input_ops() once per reader.")

        logger.info("Reading initial batch of data from input reader.")
        batch = self.next()
        if isinstance(batch, MultiAgentBatch):
            raise NotImplementedError(
                "tf_input_ops() is not implemented for multi agent batches")

        keys = [
            k for k in sorted(list(batch.keys()))
            if np.issubdtype(batch[k].dtype, np.number)
        ]
        dtypes = [batch[k].dtype for k in keys]
        shapes = {
            k: (-1, ) + s[1:]
            for (k, s) in [(k, batch[k].shape) for k in keys]
        }
        queue = tf.FIFOQueue(capacity=queue_size, dtypes=dtypes, names=keys)
        tensors = queue.dequeue()

        logger.info("Creating TF queue runner for {}".format(self))
        self._queue_runner = _QueueRunner(self, queue, keys, dtypes)
        self._queue_runner.enqueue(batch)
        self._queue_runner.start()

        out = {k: tf.reshape(t, shapes[k]) for k, t in tensors.items()}
        return out


class _QueueRunner(threading.Thread):
    """Thread that feeds a TF queue from a InputReader."""

    def __init__(self, input_reader, queue, keys, dtypes):
        threading.Thread.__init__(self)
        self.sess = tf.get_default_session()
        self.daemon = True
        self.input_reader = input_reader
        self.keys = keys
        self.queue = queue
        self.placeholders = [tf.placeholder(dtype) for dtype in dtypes]
        self.enqueue_op = queue.enqueue(dict(zip(keys, self.placeholders)))

    def enqueue(self, batch):
        data = {
            self.placeholders[i]: batch[key]
            for i, key in enumerate(self.keys)
        }
        self.sess.run(self.enqueue_op, feed_dict=data)

    def run(self):
        while True:
            try:
                batch = self.input_reader.next()
                self.enqueue(batch)
            except Exception:
                logger.exception("Error reading from input")
