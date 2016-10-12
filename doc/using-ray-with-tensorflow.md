# Using Ray with TensorFlow

This document describes best practices for using Ray with TensorFlow. If you are
training a deep network in the distributed setting, you may need to ship your
deep network between processes (or machines). For example, you may update your
model on one machine and then use that model to compute a gradient on another
machine. However, shipping the model is not always straightforward.

For example, a straightforward attempt to pickle a TensorFlow graph gives mixed
results. Some examples fail, and some succeed (but produce very large strings).
The results are similar with other pickling libraries as well.

Furthermore, creating a TensorFlow graph can take tens of seconds, and so
serializing a graph and recreating it in another process will be inefficient.
The better solution is to create the same TensorFlow graph on each worker once
at the beginning and then to ship only the weights between the workers.

Suppose we have a simple network definition (this one is modified from the
TensorFlow documentation).

```python
import tensorflow as tf
import numpy as np

x_data = tf.placeholder(tf.float32, shape=[100])
y_data = tf.placeholder(tf.float32, shape=[100])

w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
b = tf.Variable(tf.zeros([1]))
y = w * x_data + b

loss = tf.reduce_mean(tf.square(y - y_data))
optimizer = tf.train.GradientDescentOptimizer(0.5)
train = optimizer.minimize(loss)

init = tf.initialize_all_variables()
sess = tf.Session()
```

To extract the weights and set the weights, we need to write a couple lines of
boilerplate code.

```python
def get_and_set_weights_methods():
  assignment_placeholders = []
  assignment_nodes = []
  for var in tf.trainable_variables():
    assignment_placeholders.append(tf.placeholder(var.value().dtype, var.get_shape().as_list()))
    assignment_nodes.append(var.assign(assignment_placeholders[-1]))
  # Define a function for getting the network weights.
  def get_weights():
    return [v.eval(session=sess) for v in tf.trainable_variables()]
  # Define a function for setting the network weights.
  def set_weights(new_weights):
    sess.run(assignment_nodes, feed_dict={p: w for p, w in zip(assignment_placeholders, new_weights)})
  # Return the methods.
  return get_weights, set_weights

get_weights, set_weights = get_and_set_weights_methods()
```

Now we can use these methods to extract the weights, and place them back in the
network as follows.

```python
# First initialize the weights.
sess.run(init)
# Get the weights
weights = get_weights()  # Returns a list of numpy arrays
# Set the weights
set_weights(weights)
```

**Note:** If we were to set the weights using the `assign` method like below,
each call to `assign` would add a node to the graph, and the graph would grow
unmanageably large over time.

```python
w.assign(np.zeros(1))  # This adds a node to the graph every time you call it.
b.assign(np.zeros(1))  # This adds a node to the graph every time you call it.
```

## Complete Example

Putting this all together, we would first create the graph on each worker using
reusable variables. Within the reusable variables, we would define `get_weights`
and `set_weights` methods. We would then use those methods to ship the weights
(as lists of numpy arrays) between the processes without shipping the actual
TensorFlow graphs, which are much more complex Python objects.

```python
import tensorflow as tf
import numpy as np
import ray

ray.init(start_ray_local=True, num_workers=5)

BATCH_SIZE = 100
NUM_BATCHES = 1
NUM_ITERS = 201

def net_vars_initializer():
  # Seed TensorFlow to make the script deterministic.
  tf.set_random_seed(0)
  # Define the inputs.
  x_data = tf.placeholder(tf.float32, shape=[BATCH_SIZE])
  y_data = tf.placeholder(tf.float32, shape=[BATCH_SIZE])
  # Define the weights and computation.
  w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
  b = tf.Variable(tf.zeros([1]))
  y = w * x_data + b
  # Define the loss.
  loss = tf.reduce_mean(tf.square(y - y_data))
  optimizer = tf.train.GradientDescentOptimizer(0.5)
  train = optimizer.minimize(loss)
  # Define the weight initializer and session.
  init = tf.initialize_all_variables()
  sess = tf.Session()
  # Additional code for setting and getting the weights.
  def get_and_set_weights_methods():
    assignment_placeholders = []
    assignment_nodes = []
    for var in tf.trainable_variables():
      assignment_placeholders.append(tf.placeholder(var.value().dtype, var.get_shape().as_list()))
      assignment_nodes.append(var.assign(assignment_placeholders[-1]))
    def get_weights():
      return [v.eval(session=sess) for v in tf.trainable_variables()]
    def set_weights(new_weights):
      sess.run(assignment_nodes, feed_dict={p: w for p, w in zip(assignment_placeholders, new_weights)})
    return get_weights, set_weights
  get_weights, set_weights = get_and_set_weights_methods()
  # Return all of the data needed to use the network.
  return get_weights, set_weights, sess, train, loss, x_data, y_data, init

def net_vars_reinitializer(net_vars):
  return net_vars

# Define a reusable variable for the network variables.
ray.reusables.net_vars = ray.Reusable(net_vars_initializer, net_vars_reinitializer)

# Define a remote function that trains the network for one step and returns the
# new weights.
@ray.remote
def step(weights, x, y):
  get_weights, set_weights, sess, train, _, x_data, y_data, _ = ray.reusables.net_vars
  # Set the weights in the network.
  set_weights(weights)
  # Do one step of training.
  sess.run(train, feed_dict={x_data: x, y_data: y})
  # Return the new weights.
  return get_weights()

get_weights, set_weights, sess, _, loss, x_data, y_data, init = ray.reusables.net_vars
# Initialize the network weights.
sess.run(init)
# Get the weights as a list of numpy arrays.
weights = get_weights()

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

# Do some steps of training.
for iteration in range(NUM_ITERS):
  # Put the weights in the object store. This is optional. We could instead pass
  # the variable weights directly into step.remote, in which case it would be
  # placed in the object store under the hood. However, in that case multiple
  # copies of the weights would be put in the object store, so this approach is
  # more efficient.
  weights_id = ray.put(weights)
  # Call the remote function multiple times in parallel.
  new_weights_ids = [step.remote(weights_id, x_ids[i], y_ids[i]) for i in range(NUM_BATCHES)]
  # Get all of the weights.
  new_weights_list = ray.get(new_weights_ids)
  # Add up all the different weights. Each element of new_weights_list is a list
  # of weights, and we want to add up these lists component wise.
  weights = [sum(weight_tuple) / NUM_BATCHES for weight_tuple in zip(*new_weights_list)]
  # Print the current weights. They should converge to roughly to the values 0.1
  # and 0.3 used in generate_fake_x_y_data.
  if iteration % 20 == 0:
    print "Iteration {}: weights are {}".format(iteration, weights)
```
