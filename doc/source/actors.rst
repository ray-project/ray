Actors
======

Remote functions in Ray should be thought of as functional and side-effect free.
Restricting ourselves only to remote functions gives us distributed functional
programming, which is great for many use cases, but in practice is a bit
limited.

Ray extends the dataflow model with **actors**. An actor is essentially a
stateful worker (or a service). When a new actor is instantiated, a new worker
is created, and methods of the actor are scheduled on that specific worker and
can access and mutate the state of that worker.

Suppose we've already started Ray.

.. code-block:: python

  import ray
  ray.init()

Defining and creating an actor
------------------------------

An actor can be defined as follows.

.. code-block:: python

  import gym

  @ray.remote
  class GymEnvironment(object):
    def __init__(self, name):
      self.env = gym.make(name)
    def step(self, action):
      return self.env.step(action)
    def reset(self):
      self.env.reset()

Two copies of the actor can be created as follows.

.. code-block:: python

  a1 = GymEnvironment("Pong-v0")
  a2 = GymEnvironment("Pong-v0")

When the first line is run, the following happens.

- Some node in the cluster will be chosen, and a worker will be created on that
  node (by the local scheduler on that node) for the purpose of running methods
  called on the actor.
- A ``GymEnvironment`` object will be created on that worker and the
  ``GymEnvironment`` constructor will run.

When the second line is run, another node (possibly the same one) is chosen,
another worker is created on that node for the purpose of running methods called
on the second actor, and another ``GymEnvironment`` object is constructed on
the newly-created worker.

Using an actor
--------------

We can use the actor by calling one of its methods.

.. code-block:: python

  a1.step.remote(0)
  a2.step.remote(0)

When ``a1.step.remote(0)`` is called, a task is created and scheduled on the
first actor. This scheduling procedure bypasses the global scheduler, and is
assigned directly to the local scheduler responsible for the actor by the
driver's local scheduler. Since the method call is a task, ``a1.step(0)``
returns an object ID. We can call `ray.get` on the object ID to retrieve the
actual value.

The call to ``a2.step.remote(0)`` generates a task which is scheduled on the
second actor. Since these two tasks run on different actors, they can be
executed in parallel (note that only actor methods will be scheduled on actor
workers, not regular remote functions).

On the other hand, methods called on the same actor are executed serially and
share in the order that they are called and share state with one another. We
illustrate this with a simple example.

.. code-block:: python

  @ray.remote
  class Counter(object):
    def __init__(self):
      self.value = 0
    def increment(self):
      self.value += 1
      return self.value

  # Create ten actors.
  counters = [Counter() for _ in range(10)]

  # Increment each counter once and get the results. These tasks all happen in
  # parallel.
  results = ray.get([c.increment.remote() for c in counters])
  print(results)  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

  # Increment the first counter five times. These tasks are executed serially
  # and share state.
  results = ray.get([counters[0].increment.remote() for _ in range(5)])
  print(results)  # prints [2, 3, 4, 5, 6]

Using GPUs on actors
--------------------

A common use case is for an actor to contain a neural network. For example,
suppose we have a method for constructing a neural net.

.. code-block:: python

  import tensorflow as tf

  def construct_network():
    x = tf.placeholder(tf.float32, [None, 784])
    y_ = tf.placeholder(tf.float32, [None, 10])

    W = tf.Variable(tf.zeros([784, 10]))
    b = tf.Variable(tf.zeros([10]))
    y = tf.nn.softmax(tf.matmul(x, W) + b)

    cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
    train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)
    correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

    return x, y_, train_step, accuracy

We can then define an actor for this network as follows.

.. code-block:: python

  import os

  # Define an actor that runs on GPUs. If there are no GPUs, then simply use
  # ray.remote without any arguments and no parentheses.
  @ray.remote(num_gpus=1)
  class NeuralNetOnGPU(object):
    def __init__(self):
      # Set an environment variable to tell TensorFlow which GPUs to use. Note
      # that this must be done before the call to tf.Session.
      os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in ray.get_gpu_ids()])
      with tf.Graph().as_default():
        with tf.device("/gpu:0"):
          self.x, self.y_, self.train_step, self.accuracy = construct_network()
          # Allow this to run on CPUs if there aren't any GPUs.
          config = tf.ConfigProto(allow_soft_placement=True)
          self.sess = tf.Session(config=config)
          # Initialize the network.
          init = tf.global_variables_initializer()
          self.sess.run(init)

To indicate that an actor requires one GPU, we pass in ``num_gpus=1`` to
``ray.remote``. Note that in order for this to work, Ray must have been started
with some GPUs, e.g., via ``ray.init(num_gpus=2)``. Otherwise, when you try to
instantiate the GPU version with ``NeuralNetOnGPU.remote()``, an exception will
be thrown saying that there aren't enough GPUs in the system.

When the actor is created, it will have access to a list of the IDs of the GPUs
that it is allowed to use via ``ray.get_gpu_ids()``. This is a list of integers,
like ``[]``, or ``[1]``, or ``[2, 5, 6]``. Since we passed in
``ray.remote(num_gpus=1)``, this list will have length one.

We can put this all together as follows.

.. code-block:: python

  import os
  import ray
  import tensorflow as tf
  from tensorflow.examples.tutorials.mnist import input_data

  ray.init(num_gpus=8)

  def construct_network():
    x = tf.placeholder(tf.float32, [None, 784])
    y_ = tf.placeholder(tf.float32, [None, 10])

    W = tf.Variable(tf.zeros([784, 10]))
    b = tf.Variable(tf.zeros([10]))
    y = tf.nn.softmax(tf.matmul(x, W) + b)

    cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
    train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)
    correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

    return x, y_, train_step, accuracy

  @ray.remote(num_gpus=1)
  class NeuralNetOnGPU(object):
    def __init__(self, mnist_data):
      self.mnist = mnist_data
      # Set an environment variable to tell TensorFlow which GPUs to use. Note
      # that this must be done before the call to tf.Session.
      os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in ray.get_gpu_ids()])
      with tf.Graph().as_default():
        with tf.device("/gpu:0"):
          self.x, self.y_, self.train_step, self.accuracy = construct_network()
          # Allow this to run on CPUs if there aren't any GPUs.
          config = tf.ConfigProto(allow_soft_placement=True)
          self.sess = tf.Session(config=config)
          # Initialize the network.
          init = tf.global_variables_initializer()
          self.sess.run(init)

    def train(self, num_steps):
      for _ in range(num_steps):
        batch_xs, batch_ys = self.mnist.train.next_batch(100)
        self.sess.run(self.train_step, feed_dict={self.x: batch_xs, self.y_: batch_ys})

    def get_accuracy(self):
      return self.sess.run(self.accuracy, feed_dict={self.x: self.mnist.test.images,
                                                     self.y_: self.mnist.test.labels})


  # Load the MNIST dataset and tell Ray how to serialize the custom classes.
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  ray.register_class(type(mnist))
  ray.register_class(type(mnist.train))

  # Create the actor.
  nn = NeuralNetOnGPU.remote(mnist)

  # Run a few steps of training and print the accuracy.
  nn.train.remote(100)
  accuracy = ray.get(nn.get_accuracy.remote())
  print("Accuracy is {}.".format(accuracy))
