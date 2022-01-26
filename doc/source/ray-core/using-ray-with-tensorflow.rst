Best Practices: Ray with Tensorflow
===================================

This document describes best practices for using the Ray core APIs with TensorFlow. Ray also provides higher-level utilities for working with Tensorflow, such as distributed training APIs (`training tensorflow example`_), Tune for hyperparameter search (:doc:`/tune/examples/tf_mnist_example`), RLlib for reinforcement learning (`RLlib tensorflow example`_).

.. _`training tensorflow example`: tf_distributed_training.html
.. _`RLlib tensorflow example`: rllib-models.html#tensorflow-models

Feel free to contribute if you think this document is missing anything.


Common Issues: Pickling
-----------------------

One common issue with TensorFlow2.0 is a pickling error like the following:

.. code-block:: none

    File "/home/***/venv/lib/python3.6/site-packages/ray/actor.py", line 322, in remote
      return self._remote(args=args, kwargs=kwargs)
    File "/home/***/venv/lib/python3.6/site-packages/ray/actor.py", line 405, in _remote
      self._modified_class, self._actor_method_names)
    File "/home/***/venv/lib/python3.6/site-packages/ray/function_manager.py", line 578, in export_actor_class
      "class": pickle.dumps(Class),
    File "/home/***/venv/lib/python3.6/site-packages/ray/cloudpickle/cloudpickle.py", line 1123, in dumps
      cp.dump(obj)
    File "/home/***/lib/python3.6/site-packages/ray/cloudpickle/cloudpickle.py", line 482, in dump
      return Pickler.dump(self, obj)
    File "/usr/lib/python3.6/pickle.py", line 409, in dump
      self.save(obj)
    File "/usr/lib/python3.6/pickle.py", line 476, in save
      f(self, obj) # Call unbound method with explicit self
    File "/usr/lib/python3.6/pickle.py", line 751, in save_tuple
      save(element)
    File "/usr/lib/python3.6/pickle.py", line 808, in _batch_appends
      save(tmp[0])
    File "/usr/lib/python3.6/pickle.py", line 496, in save
      rv = reduce(self.proto)
    TypeError: can't pickle _LazyLoader objects

To resolve this, you should move all instances of ``import tensorflow`` into the Ray actor or function, as follows:

.. code-block:: python

    def create_model():
        import tensorflow as tf
        ...

This issue is caused by side-effects of importing TensorFlow and setting global state.


Use Actors for Parallel Models
------------------------------

If you are training a deep network in the distributed setting, you may need to
ship your deep network between processes (or machines). However, shipping the model is not always straightforward.

.. tip::
  Avoid sending the Tensorflow model directly. A straightforward attempt to pickle a TensorFlow graph gives mixed results. Furthermore, creating a TensorFlow graph can take tens of seconds, and so serializing a graph and recreating it in another process will be inefficient.

It is recommended to replicate the same TensorFlow graph on each worker once
at the beginning and then to ship only the weights between the workers.

Suppose we have a simple network definition (this one is modified from the
TensorFlow documentation).

.. literalinclude:: /ray-core/_examples/doc_code/tf_example.py
   :language: python
   :start-after: __tf_model_start__
   :end-before: __tf_model_end__

It is strongly recommended you create actors to handle this. To do this, first initialize
ray and define an Actor class:

.. literalinclude:: /ray-core/_examples/doc_code/tf_example.py
   :language: python
   :start-after: __ray_start__
   :end-before: __ray_end__

Then, we can instantiate this actor and train it on the separate process:

.. literalinclude:: /ray-core/_examples/doc_code/tf_example.py
   :language: python
   :start-after: __actor_start__
   :end-before: __actor_end__


We can then use ``set_weights`` and ``get_weights`` to move the weights of the neural network
around. This allows us to manipulate weights between different models running in parallel without shipping the actual TensorFlow graphs, which are much more complex Python objects.


.. literalinclude:: /ray-core/_examples/doc_code/tf_example.py
   :language: python
   :start-after: __weight_average_start__


Lower-level TF Utilities
------------------------
Given a low-level TF definition:

.. code-block:: python

  import tensorflow as tf
  import numpy as np

  x_data = tf.placeholder(tf.float32, shape=[100])
  y_data = tf.placeholder(tf.float32, shape=[100])

  w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
  b = tf.Variable(tf.zeros([1]))
  y = w * x_data + b

  loss = tf.reduce_mean(tf.square(y - y_data))
  optimizer = tf.train.GradientDescentOptimizer(0.5)
  grads = optimizer.compute_gradients(loss)
  train = optimizer.apply_gradients(grads)

  init = tf.global_variables_initializer()
  sess = tf.Session()

To extract the weights and set the weights, you can use the following helper
method.

.. code-block:: python

  import ray.experimental.tf_utils
  variables = ray.experimental.tf_utils.TensorFlowVariables(loss, sess)

The ``TensorFlowVariables`` object provides methods for getting and setting the
weights as well as collecting all of the variables in the model.

Now we can use these methods to extract the weights, and place them back in the
network as follows.

.. code-block:: python

  sess = tf.Session()
  # First initialize the weights.
  sess.run(init)
  # Get the weights
  weights = variables.get_weights()  # Returns a dictionary of numpy arrays
  # Set the weights
  variables.set_weights(weights)

**Note:** If we were to set the weights using the ``assign`` method like below,
each call to ``assign`` would add a node to the graph, and the graph would grow
unmanageably large over time.

.. code-block:: python

  w.assign(np.zeros(1))  # This adds a node to the graph every time you call it.
  b.assign(np.zeros(1))  # This adds a node to the graph every time you call it.


.. note:: This may not work with `tf.Keras`.



Troubleshooting
~~~~~~~~~~~~~~~

Note that ``TensorFlowVariables`` uses variable names to determine what
variables to set when calling ``set_weights``. One common issue arises when two
networks are defined in the same TensorFlow graph. In this case, TensorFlow
appends an underscore and integer to the names of variables to disambiguate
them. This will cause ``TensorFlowVariables`` to fail. For example, if we have a
class definiton ``Network`` with a ``TensorFlowVariables`` instance:

.. code-block:: python

  import ray
  import tensorflow as tf

  class Network(object):
      def __init__(self):
          a = tf.Variable(1)
          b = tf.Variable(1)
          c = tf.add(a, b)
          sess = tf.Session()
          init = tf.global_variables_initializer()
          sess.run(init)
          self.variables = ray.experimental.tf_utils.TensorFlowVariables(c, sess)

      def set_weights(self, weights):
          self.variables.set_weights(weights)

      def get_weights(self):
          return self.variables.get_weights()

and run the following code:

.. code-block:: python

  a = Network()
  b = Network()
  b.set_weights(a.get_weights())

the code would fail. If we instead defined each network in its own TensorFlow
graph, then it would work:

.. code-block:: python

  with tf.Graph().as_default():
      a = Network()
  with tf.Graph().as_default():
      b = Network()
  b.set_weights(a.get_weights())

This issue does not occur between actors that contain a network, as each actor
is in its own process, and thus is in its own graph. This also does not occur
when using ``set_flat``.

Another issue to keep in mind is that ``TensorFlowVariables`` needs to add new
operations to the graph. If you close the graph and make it immutable, e.g.
creating a ``MonitoredTrainingSession`` the initialization will fail. To resolve
this, simply create the instance before you close the graph.
