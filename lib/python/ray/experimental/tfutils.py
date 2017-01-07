from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

class TFVariables(object):
  """An object used to extract variables from a loss function, and provide 
     methods for getting and setting the weights of said variables.

  tf is the tensorflow library.

  Attributes:
    sess (tf.Session): The tensorflow session used to run assignment.
    loss: The loss function passed in by the user.
    variables (List[tf.Variable]): Extracted variables from the loss.
    assignment_placeholders (List[tf.placeholders]): The nodes that weights get passed to.
    assignment_nodes (List[tf.Tensor]): The nodes that assign the weights.
  """
  def __init__(self, loss, sess):
    """Creates a TFVariables instance."""
    import tensorflow as tf
    self.sess = sess

    # v.name[:-7] strips the /Assign from the name as we want the 
    # general variable rather than the assign node. 
    var_names = [v.name[:-7] for v in loss.graph.get_operations() if v.name.endswith("/Assign")]                                                                                                  
    # Split is required as tensorflow appends :0 to a variable name.
    self.variables = [v for v in tf.trainable_variables() if v.name.split(":")[0] in var_names]     
    assignment_placeholders = []
    assignment_nodes = []
    
    # Create new placeholders to put in custom weights.
    for var in self.variables:
      assignment_placeholders.append(tf.placeholder(var.value().dtype, var.get_shape().as_list()))
      assignment_nodes.append(var.assign(assignment_placeholders[-1]))
    self.loss = loss
    self.assignment_placeholders = assignment_placeholders
    self.assignment_nodes = assignment_nodes

  def get_weights(self):
    """Returns the weights of the variables of the loss function in a list."""
    return [v.eval(session=self.sess) for v in self.variables]

  def set_weights(self, new_weights):
    """Sets the weights to new_weights."""
    self.sess.run(self.assignment_nodes, feed_dict={p: w for p, w in zip(self.assignment_placeholders, new_weights)})
