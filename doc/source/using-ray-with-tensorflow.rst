Best Practices: Ray with Tensorflow
===================================

This document describes best practices for using Ray with TensorFlow.

Use Actors for  Model
-----------------------

If you are training a deep network in the distributed setting, you may need to
ship your deep network between processes (or machines). However, shipping the model is not always straightforward.

.. tip::
  Avoid sending the Tensorflow model directly. A straightforward attempt to pickle a TensorFlow graph gives mixed results.  Furthermore, creating a TensorFlow graph can take tens of seconds, and so serializing a graph and recreating it in another process will be inefficient.

It is recommended to replicate the same TensorFlow graph on each worker once
at the beginning and then to ship only the weights between the workers.

Suppose we have a simple network definition (this one is modified from the
TensorFlow documentation).

.. code-block:: python

  import tensorflow as tf
  from tensorflow.keras import layers

  def create_keras_model():
      model = tf.keras.Sequential()
      # Adds a densely-connected layer with 64 units to the model:
      model.add(layers.Dense(64, activation='relu'))
      # Add another:
      model.add(layers.Dense(64, activation='relu'))
      # Add a softmax layer with 10 output units:
      model.add(layers.Dense(10, activation='softmax'))

      model.compile(optimizer=tf.train.RMSPropOptimizer(0.01),
                    loss=tf.keras.losses.categorical_crossentropy,
                    metrics=[tf.keras.metrics.categorical_accuracy])
      return model

It is strongly recommended you create actors to handle this:

.. code-block:: python

  import ray
  import numpy as np

  ray.init()

  def random_one_hot_labels(shape):
    n, n_class = shape
    classes = np.random.randint(0, n_class, n)
    labels = np.zeros((n, n_class))
    labels[np.arange(n), classes] = 1
    return labels

  # num_gpus is optional
  @ray.remote(num_gpus=1)
  class Network():
      def __init__(self):
          self.model = create_keras_model()
          self.dataset = np.random.random((1000, 32))
          self.labels = random_one_hot_labels((1000, 10))

      def train(self):
          return self.model.fit(self.dataset, self.labels)

      def get_weights(self):
          return self.model.get_weights()

      def set_weights(self, weights):
          # Note that for simplicity this does not handle the optimizer state.
          return self.model.set_weights(weights)

  NetworkActor = Network.remote()
  result_object_id = NetworkActor.train.remote()
  ray.get(result_object_id)


Example for Distributed Weight Averaging
----------------------------------------

Putting this all together, we would first embed the graph in an actor. Within
the actor, we would use the ``get_weights`` and ``set_weights`` methods of the
``TensorFlowVariables`` class. We would then use those methods to ship the weights
(as a dictionary of variable names mapping to numpy arrays) between the
processes without shipping the actual TensorFlow graphs, which are much more
complex Python objects.

.. literalinclude:: ...

    TODO


How to Train in Parallel using Ray and Gradients
------------------------------------------------

In some cases, you may want to do data-parallel training on your network. We use the network
above to illustrate how to do this in Ray. The only differences are in the remote function
``step`` and the driver code.

In the function ``step``, we run the grad operation rather than the train operation to get the gradients.
Since Tensorflow pairs the gradients with the variables in a tuple, we extract the gradients to avoid
needless computation.

Extracting numerical gradients
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Code like the following can be used in a remote function to compute numerical gradients.

.. code-block:: python

  x_values = [1] * 100
  y_values = [2] * 100
  numerical_grads = sess.run([grad[0] for grad in grads], feed_dict={x_data: x_values, y_data: y_values})

Using the returned gradients to train the network
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By pairing the symbolic gradients with the numerical gradients in a feed_dict, we can update the network.

.. code-block:: python

  # We can feed the gradient values in using the associated symbolic gradient
  # operation defined in tensorflow.
  feed_dict = {grad[0]: numerical_grad for (grad, numerical_grad) in zip(grads, numerical_grads)}
  sess.run(train, feed_dict=feed_dict)

You can then run ``variables.get_weights()`` to see the updated weights of the network.

For reference, the full code is below:

.. code-block:: python

  import tensorflow as tf
  import numpy as np
  import ray

  ray.init()

  BATCH_SIZE = 100
  NUM_BATCHES = 1
  NUM_ITERS = 201

  class Network(object):
      def __init__(self, x, y):
          # Seed TensorFlow to make the script deterministic.
          tf.set_random_seed(0)
          # Define the inputs.
          x_data = tf.constant(x, dtype=tf.float32)
          y_data = tf.constant(y, dtype=tf.float32)
          # Define the weights and computation.
          w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
          b = tf.Variable(tf.zeros([1]))
          y = w * x_data + b
          # Define the loss.
          self.loss = tf.reduce_mean(tf.square(y - y_data))
          optimizer = tf.train.GradientDescentOptimizer(0.5)
          self.grads = optimizer.compute_gradients(self.loss)
          self.train = optimizer.apply_gradients(self.grads)
          # Define the weight initializer and session.
          init = tf.global_variables_initializer()
          self.sess = tf.Session()
          # Additional code for setting and getting the weights
          self.variables = ray.experimental.tf_utils.TensorFlowVariables(self.loss, self.sess)
          # Return all of the data needed to use the network.
          self.sess.run(init)

      # Define a remote function that trains the network for one step and returns the
      # new weights.
      def step(self, weights):
          # Set the weights in the network.
          self.variables.set_weights(weights)
          # Do one step of training. We only need the actual gradients so we filter over the list.
          actual_grads = self.sess.run([grad[0] for grad in self.grads])
          return actual_grads

      def get_weights(self):
          return self.variables.get_weights()

  # Define a remote function for generating fake data.
  @ray.remote(num_return_vals=2)
  def generate_fake_x_y_data(num_data, seed=0):
      # Seed numpy to make the script deterministic.
      np.random.seed(seed)
      x = np.random.rand(num_data)
      y = x * 0.1 + 0.3
      return x, y

  # Generate some training data.
  batch_ids = [generate_fake_x_y_data.remote(BATCH_SIZE, seed=i) for i in range(NUM_BATCHES)]
  x_ids = [x_id for x_id, y_id in batch_ids]
  y_ids = [y_id for x_id, y_id in batch_ids]
  # Generate some test data.
  x_test, y_test = ray.get(generate_fake_x_y_data.remote(BATCH_SIZE, seed=NUM_BATCHES))

  # Create actors to store the networks.
  remote_network = ray.remote(Network)
  actor_list = [remote_network.remote(x_ids[i], y_ids[i]) for i in range(NUM_BATCHES)]
  local_network = Network(x_test, y_test)

  # Get initial weights of local network.
  weights = local_network.get_weights()

  # Do some steps of training.
  for iteration in range(NUM_ITERS):
      # Put the weights in the object store. This is optional. We could instead pass
      # the variable weights directly into step.remote, in which case it would be
      # placed in the object store under the hood. However, in that case multiple
      # copies of the weights would be put in the object store, so this approach is
      # more efficient.
      weights_id = ray.put(weights)
      # Call the remote function multiple times in parallel.
      gradients_ids = [actor.step.remote(weights_id) for actor in actor_list]
      # Get all of the weights.
      gradients_list = ray.get(gradients_ids)

      # Take the mean of the different gradients. Each element of gradients_list is a list
      # of gradients, and we want to take the mean of each one.
      mean_grads = [sum([gradients[i] for gradients in gradients_list]) / len(gradients_list) for i in range(len(gradients_list[0]))]

      feed_dict = {grad[0]: mean_grad for (grad, mean_grad) in zip(local_network.grads, mean_grads)}
      local_network.sess.run(local_network.train, feed_dict=feed_dict)
      weights = local_network.get_weights()

      # Print the current weights. They should converge to roughly to the values 0.1
      # and 0.3 used in generate_fake_x_y_data.
      if iteration % 20 == 0:
          print("Iteration {}: weights are {}".format(iteration, weights))


Lower-level TF Utilities
------------------------

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


.. autoclass:: ray.experimental.tf_utils.TensorFlowVariables
   :members:


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
