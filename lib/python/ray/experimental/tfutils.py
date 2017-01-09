from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

class TensorFlowVariables(object):
  """An object used to extract variables from a loss function, and provide 
     methods for getting and setting the weights of said variables.

  Attributes:
    sess (tf.Session): The tensorflow session used to run assignment.
    loss: The loss function passed in by the user.
    variables (List[tf.Variable]): Extracted variables from the loss.
    assignment_placeholders (List[tf.placeholders]): The nodes that weights get passed to.
    assignment_nodes (List[tf.Tensor]): The nodes that assign the weights.
  """
  def __init__(self, loss, sess=None):
    """Creates a TensorFlowVariables instance."""
    import tensorflow as tf
    self.sess = sess
    self.loss = loss
    variable_names = [op.node_def.name for op in loss.graph.get_operations() if op.node_def.op == "Variable"]
    self.variables = [v for v in tf.trainable_variables() if v.op.node_def.name in variable_names]
    self.assignment_placeholders = dict()
    self.assignment_nodes = []
    
    # Create new placeholders to put in custom weights.
    for var in self.variables:
      self.assignment_placeholders[var.op.node_def.name] = tf.placeholder(var.value().dtype, var.get_shape().as_list())
      self.assignment_nodes.append(var.assign(self.assignment_placeholders[var.op.node_def.name]))

  def set_session(self, sess):
    """Modifies the current session used by the class"""
    self.sess = sess

  def get_weights(self):
    """Returns the weights of the variables of the loss function in a list."""
    assert self.sess is not None, "Session is not set. User should set the session through either the initialization or by calling set_session(sess)."
    return {v.op.node_def.name: v.eval(session=self.sess) for v in self.variables}

  def set_weights(self, new_weights):
    """Sets the weights to new_weights."""
    assert self.sess is not None, "Session is not set. User should set the session through either the initialization or by calling set_session(sess)."
    self.sess.run(self.assignment_nodes, feed_dict={self.assignment_placeholders[name]: value for (name, value) in new_weights.items()})
