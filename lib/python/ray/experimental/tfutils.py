from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

class TFVariables(object):
  def __init__(self, loss, sess):
    import tensorflow as tf
    self.sess = sess

    # v.name[:-7] strips the /Assign from the name as we want the 
    # general variable rather than the assign node. 
    var_names = [v.name[:-7] for v in loss.graph.get_operations() if v.name.endswith("/Assign")]                                                                                                  
    # Split is required as tensorflow appends :0 to a variable name
    self.variables = [v for v in tf.trainable_variables() if v.name.split(":")[0] in var_names]     
    assignment_placeholders = []
    assignment_nodes = []
    
    # Create new placeholders to put in custom weights
    for var in self.variables:
      assignment_placeholders.append(tf.placeholder(var.value().dtype, var.get_shape().as_list()))
      assignment_nodes.append(var.assign(assignment_placeholders[-1]))
    self.loss = loss
    self.assignment_placeholders = assignment_placeholders
    self.assignment_nodes = assignment_nodes

  def get_weights(self):
    return [v.eval(session=self.sess) for v in self.variables]

  def set_weights(self, new_weights):
    self.sess.run(self.assignment_nodes, feed_dict={p: w for p, w in zip(self.assignment_placeholders, new_weights)})
