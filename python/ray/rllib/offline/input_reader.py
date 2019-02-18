from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import threading

from ray.rllib.utils.annotations import PublicAPI
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

        Example:
            >>> class MyModel(rllib.model.Model):
            ...     def custom_loss(self, policy_loss):
            ...         reader = JsonReader(...)
            ...         input_ops = reader.tf_input_ops()
            ...         self.scope.reuse_variables()
            ...         logits, _ = self._build_layers_v2(
            ...             _restore_original_dimensions(
            ...                 {"obs": input_ops["obs"]}, self.obs_space),
            ...                 self.num_outputs, self.options)
            ...         il_loss = imitation_loss(logits, input_ops["action"])
            ...         return policy_loss + il_loss
            
        You can find a runnable version of this in examples/supervised_loss.py.
        
        Returns:
            dict of Tensors (single-agent), or dict keyed by policy ids of
            these Tensors (multi-agent mode).
        """

        if hasattr(self, "_queue_runner"):
            raise ValueError(
                "A queue runner already exists for this input reader. "
                "You can only call tf_input_ops() once per reader.")
        batch = self.next()
        keys = sorted(list(batch.keys()))
        dtypes = [batch[k].dtype for k in keys]
        queue = tf.FIFOQueue(capacity=queue_size, dtypes=dtypes, names=keys)
        tensors = queue.dequeue()
        self._queue_runner = _QueueRunner(self, queue, keys, dtypes)
        self._queue_runner.start()
        return {
            k: tensors[i] for i, k in enumerate(keys)
        }


class _QueueRunner(threading.Thread):
    def __init__(self, input_reader, queue, keys, dtypes)
        threading.Thread.__init__(self)
        self.sess = tf.get_default_session()
        self.daemon = True
        self.input_reader = input_reader
        self.keys = keys
        self.queue = queue
        self.placeholders = [tf.Placeholder(dtype) for dtype in dtypes]
        self.enqueue_op = queue.enqueue(self.placeholders)

    def run(self):
        while True:
            try:
                batch = self.input_reader.next()
                self.sess.run(
                    self.enqueue_op, feed_dict={
                        self.placeholders[i]: batch[keys[i]]
                        for i in range(self.keys)
                    })
            except:
                logger.exception("Error reading from input")
