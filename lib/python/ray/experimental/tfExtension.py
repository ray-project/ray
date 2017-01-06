try:
  import tensorflow as tf

  class TFVariables(object):
    def __init__(self, loss, sess):
      self.sess = sess
      var_names = [v.name[:-7] + ":0" for v in loss.graph.get_operations() if v.name.endswith('/Assign')]
      self.variables = [v for v in tf.trainable_variables() if v.name in var_names]
      assignment_placeholders = []
      assignment_nodes = []
      for var in self.variables:
        self.sess.run(var.initializer)
        assignment_placeholders.append(tf.placeholder(var.value().dtype, var.get_shape().as_list()))
        assignment_nodes.append(var.assign(assignment_placeholders[-1]))
      self.loss = loss
      self.assignment_placeholders = assignment_placeholders
      self.assignment_nodes = assignment_nodes

    def get_weights(self):
      return [v.eval(session=self.sess) for v in self.variables]

    def set_weights(self, new_weights):
      self.sess.run(self.assignment_nodes, feed_dict={p: w for p, w in zip(self.assignment_placeholders, new_weights)})

except ImportError:
  pass
