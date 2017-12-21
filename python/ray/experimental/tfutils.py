from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import numpy as np
from collections import deque, OrderedDict


def unflatten(vector, shapes):
    i = 0
    arrays = []
    for shape in shapes:
        size = np.prod(shape, dtype=np.int)
        array = vector[i:(i + size)].reshape(shape)
        arrays.append(array)
        i += size
    assert len(vector) == i, "Passed weight does not have the correct shape."
    return arrays


class TensorFlowVariables(object):
    """An object used to extract variables from a loss function.

    This object also provides methods for getting and setting the weights of
    the relevant variables.

    Attributes:
        sess (tf.Session): The tensorflow session used to run assignment.
        loss: The loss function passed in by the user.
        variables (List[tf.Variable]): Extracted variables from the loss.
        assignment_placeholders (List[tf.placeholders]): The nodes that weights
            get passed to.
      assignment  _nodes (List[tf.Tensor]): The nodes that assign the weights.
    """
    def __init__(self, loss, sess=None):
        """Creates a TensorFlowVariables instance."""
        import tensorflow as tf
        self.sess = sess
        self.loss = loss
        queue = deque([loss])
        variable_names = []
        explored_inputs = set([loss])

        # We do a BFS on the dependency graph of the input function to find
        # the variables.
        while len(queue) != 0:
            tf_obj = queue.popleft()

            # The object put into the queue is not necessarily an operation, so
            # we want the op attribute to get the operation underlying the
            # object. Only operations contain the inputs that we can explore.
            if hasattr(tf_obj, "op"):
                tf_obj = tf_obj.op
            for input_op in tf_obj.inputs:
                if input_op not in explored_inputs:
                    queue.append(input_op)
                    explored_inputs.add(input_op)
            # Tensorflow control inputs can be circular, so we keep track of
            # explored operations.
            for control in tf_obj.control_inputs:
                if control not in explored_inputs:
                    queue.append(control)
                    explored_inputs.add(control)
            if "Variable" in tf_obj.node_def.op:
                variable_names.append(tf_obj.node_def.name)
        self.variables = OrderedDict()
        for v in [v for v in tf.global_variables()
                  if v.op.node_def.name in variable_names]:
            self.variables[v.op.node_def.name] = v
        self.placeholders = dict()
        self.assignment_nodes = []

        # Create new placeholders to put in custom weights.
        for k, var in self.variables.items():
            self.placeholders[k] = tf.placeholder(var.value().dtype,
                                                  var.get_shape().as_list())
            self.assignment_nodes.append(var.assign(self.placeholders[k]))

    def set_session(self, sess):
        """Modifies the current session used by the class."""
        self.sess = sess

    def get_flat_size(self):
        return sum([np.prod(v.get_shape().as_list())
                   for v in self.variables.values()])

    def _check_sess(self):
        """Checks if the session is set, and if not throw an error message."""
        assert self.sess is not None, ("The session is not set. Set the "
                                       "session either by passing it into the "
                                       "TensorFlowVariables constructor or by "
                                       "calling set_session(sess).")

    def get_flat(self):
        """Gets the weights and returns them as a flat array."""
        self._check_sess()
        return np.concatenate([v.eval(session=self.sess).flatten()
                               for v in self.variables.values()])

    def set_flat(self, new_weights):
        """Sets the weights to new_weights, converting from a flat array."""
        self._check_sess()
        shapes = [v.get_shape().as_list() for v in self.variables.values()]
        arrays = unflatten(new_weights, shapes)
        placeholders = [self.placeholders[k]
                        for k, v in self.variables.items()]
        self.sess.run(self.assignment_nodes,
                      feed_dict=dict(zip(placeholders, arrays)))

    def get_weights(self):
        """Returns a list of the weights of the loss function variables."""
        self._check_sess()
        return {k: v.eval(session=self.sess)
                for k, v in self.variables.items()}

    def set_weights(self, new_weights):
        """Sets the weights to new_weights."""
        self._check_sess()
        self.sess.run(self.assignment_nodes,
                      feed_dict={self.placeholders[name]: value
                                 for (name, value) in new_weights.items()
                                 if name in self.placeholders})
