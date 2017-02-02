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

init = tf.global_variables_initializer()
sess = tf.Session()
```

To extract the weights and set the weights, you can use the following helper
method.

```python
import ray
variables = ray.experimental.TensorFlowVariables(loss, sess)
```

The `TensorFlowVariables` object provides methods for getting and setting the
weights as well as collecting all of the variables in the model.

Now we can use these methods to extract the weights, and place them back in the
network as follows.

```python
# First initialize the weights.
sess.run(init)
# Get the weights
weights = variables.get_weights()  # Returns a dictionary of numpy arrays
# Set the weights
variables.set_weights(weights)
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
environment variables. Within the environment variables, we would use the
`get_weights` and `set_weights` methods of the `TensorFlowVariables` class. We
would then use those methods to ship the weights (as a dictionary of variable
names mapping to tensorflow tensors) between the processes without shipping the
actual TensorFlow graphs, which are much more complex Python objects. Note that
to avoid namespace collision with already created variables on the workers, we
use a separate graph for each network.

```python
import tensorflow as tf
import numpy as np
import ray

ray.init(num_workers=5)

BATCH_SIZE = 100
NUM_BATCHES = 1
NUM_ITERS = 201

def net_vars_initializer():
  # Use a separate graph for each network.
  with tf.Graph().as_default():
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
    grad = optimizer.compute_gradients(loss)
    train = optimizer.apply_gradients(grad)
    # Define the weight initializer and session.
    init = tf.global_variables_initializer()
    sess = tf.Session()
    # Additional code for setting and getting the weights
    variables = ray.experimental.TensorFlowVariables(loss, sess)
    # Return all of the data needed to use the network.
  return variables, sess, grad, apply, loss, x_data, y_data, init

def net_vars_reinitializer(net_vars):
  return net_vars

# Define an environment variable for the network variables.
ray.env.net_vars = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)

# Define a remote function that trains the network for one step and returns the
# new weights.
@ray.remote
def step(weights, x, y):
  variables, sess, _, train, _, x_data, y_data, _ = ray.env.net_vars
  # Set the weights in the network.
  variables.set_weights(weights)
  # Do one step of training.
  sess.run(train, feed_dict={x_data: x, y_data: y})
  # Return the new weights.
  return variables.get_weights()

variables, sess, _, train, loss, x_data, y_data, init = ray.env.net_vars
# Initialize the network weights.
sess.run(init)
# Get the weights as a dictionary of numpy arrays.
weights = variables.get_weights()

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
  # Add up all the different weights. Each element of new_weights_list is a dict
  # of weights, and we want to add up these dicts component wise using the keys
  # of the first dict.
  weights = {variable: sum(weight_dict[variable] for weight_dict in new_weights_list) / NUM_BATCHES for variable in new_weights_list[0]}
  # Print the current weights. They should converge to roughly to the values 0.1
  # and 0.3 used in generate_fake_x_y_data.
  if iteration % 20 == 0:
    print("Iteration {}: weights are {}".format(iteration, weights))
```

## How to Train in Parallel using Ray

In some cases, you may want to do data-parallel training on your network. We use the network 
above to illustrate how to do this in Ray. The only differences are in the step remote 
function and the driver code.

In the step function, we run the grad operation rather than the train operation to get the gradients.
Since Tensorflow pairs the gradients with the variables, we extract the gradients.

```python
  grads = sess.run(grad, feed_dict={x_data: x, y_data: y})
  # We only need the actual gradients.
  return [grad[0] for grad in grads]
```

In the main driver code, we get the symbolic gradients from the same operation run in step. These will
be used in the feed_dict to the apply gradients. We then take the mean of the gradients returned by step,
and then create a feed dict to apply the gradients.

```python
  sym_grads = [grad[0] for grad in grads]
  mean_grads = [sum([gradients[i] for gradients in gradients_list]) / len(gradients_list) for i in range(len(gradients_list[0]))]
  feedDict = dict(zip(sym_grads, mean_grads))
  sess.run(apply, feed_dict=feedDict)
```

For reference, the full code is below:

```python
import tensorflow as tf
import numpy as np
import ray

ray.init(num_workers=5)

BATCH_SIZE = 100
NUM_BATCHES = 1
NUM_ITERS = 201

def net_vars_initializer():
  # Use a separate graph for each network.
  with tf.Graph().as_default():
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
    grad = optimizer.compute_gradients(loss)
    train = optimizer.apply_gradients(grad)

    # Define the weight initializer and session.
    init = tf.global_variables_initializer()
    sess = tf.Session()
    # Additional code for setting and getting the weights
    variables = ray.experimental.TensorFlowVariables(loss, sess)
    # Return all of the data needed to use the network.
  return variables, sess, grad, apply, loss, x_data, y_data, init

def net_vars_reinitializer(net_vars):
  return net_vars

# Define an environment variable for the network variables.
ray.env.net_vars = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)

# Define a remote function that trains the network for one step and returns the
# new weights.
@ray.remote
def step(weights, x, y):
  variables, sess, grad, _, _, x_data, y_data, _ = ray.env.net_vars
  # Set the weights in the network.
  variables.set_weights(weights)
  # Do one step of training.
  grads = sess.run(grad, feed_dict={x_data: x, y_data: y})
  # We only need the actual gradients.
  return [grad[0] for grad in grads]

variables, sess, grads, apply, loss, x_data, y_data, init = ray.env.net_vars
# Initialize the network weights.
sess.run(init)
# Get the weights as a dictionary of numpy arrays.
weights = variables.get_weights()

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

sym_grads = [grad[0] for grad in grads]

# Do some steps of training.
for iteration in range(NUM_ITERS):
  # Put the weights in the object store. This is optional. We could instead pass
  # the variable weights directly into step.remote, in which case it would be
  # placed in the object store under the hood. However, in that case multiple
  # copies of the weights would be put in the object store, so this approach is
  # more efficient.
  weights_id = ray.put(weights)
  # Call the remote function multiple times in parallel.
  gradients_ids = [step.remote(weights_id, x_ids[i], y_ids[i]) for i in range(NUM_BATCHES)]
  # Get all of the weights.
  gradients_list = ray.get(gradients_ids)

  # Take the mean of the different gradients. Each element of gradients_list is a list
  # of gradients, and we want to take the mean of each one.
  mean_grads = [sum([gradients[i] for gradients in gradients_list]) / len(gradients_list) for i in range(len(gradients_list[0]))]
  feedDict = dict(zip(sym_grads, mean_grads))
  sess.run(apply, feed_dict=feedDict)
  weights = variables.get_weights()

  # Print the current weights. They should converge to roughly to the values 0.1
  # and 0.3 used in generate_fake_x_y_data.
  if iteration % 20 == 0:
    print("Iteration {}: weights are {}".format(iteration, weights))
```
