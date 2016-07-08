## Batch L-BFGS

This document provides a walkthrough of the L-BFGS example. To run the
application, first install these dependencies.

- SciPy
- [TensorFlow](https://www.tensorflow.org/).

Then from the directory `ray/examples/lbfgs/` run the following.

```
source ../../setup-env.sh
python driver.py
```

Optimization is at the heart of many machine learning algorithms. Much of
machine learning involves specifying a loss function and finding the parameters
that minimize the loss. If we can compute the gradient of the loss function,
then we can apply a variety of gradient-based optimization algorithms. L-BFGS is
one such algorithm.

### The serial version

First we load the data in batches. Here, each element in `batches` is a tuple
whose first component is a batch of `100` images and whose second component is a
batch of the `100` corresponding labels. For simplicity, we use TensorFlow's
built in methods for loading the data.

```python
from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

batch_size = 100
num_batches = mnist.train.num_examples / batch_size
batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]
```

Now, suppose we have defined a function which takes a set of model parameters
`theta` and a batch of data (both images and labels) and computes the loss for
that choice of model parameters on that batch of data. Similarly, suppose we've
also defined a function that takes the same arguments and computes the gradient
of the loss for that choice of model parameters.

```python
def loss(theta, xs, ys):
  # compute the loss on a batch of data
  return loss

def grad(theta, xs, ys):
  # compute the gradient on a batch of data
  return grad

def full_loss(theta):
  # compute the loss on the full data set
  return sum([loss(theta, xs, ys) for (xs, ys) in batches])

def full_grad(theta):
  # compute the gradient on the full data set
  return sum([grad(theta, xs, ys) for (xs, ys) in batches])
```

Since we are working with a small dataset, we don't actually need to separate
these methods into the part that operates on a batch and the part that operates
on the full dataset, but doing so will make the distributed version clearer.

Now, if we wish to optimize the loss function using L-BFGS, we simply plug these
functions, along with an initial choice of model parameters, into
`scipy.optimize.fmin_l_bfgs_b`.

```python
theta_init = np.zeros(functions.dim)
result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, fprime=full_grad)
```

### The distributed version

The extent to which this computation can be parallelized depends on the
specifics of the optimization problem, but for large datasets, the computation
of the gradient itself is often embarrassingly parallel.

To run this example in Ray, we use three files.

- [driver.py](driver.py) - This is the script that gets run. It launches the
  remote tasks and retrieves the results. The application can be run with
  `python driver.py`.
- [functions.py](functions.py) - This is the file that defines the remote
  functions (in this case, just `loss` and `grad`).
- [worker.py](worker.py) - This is the Python code that each worker process
  runs. It imports the relevant modules and tells the scheduler what functions
  it knows how to execute. Then it enters a loop that waits to receive tasks
  from the scheduler.

First, let's turn the data into a collection of remote objects.

```python
batch_refs = [ray.put(xs), ray.put(ys) for (xs, ys) in batches]
```

MNIST easily fits on a single machine, but for larger data sets, we will need to
use remote functions to distribute the loading of the data.

Now, lets turn `loss` and `grad` into remote functions. In the example
application, this is done in [functions.py](functions.py).

```python
@ray.remote([np.ndarray, np.ndarray, np.ndarray], [float])
def loss(theta, xs, ys):
  # compute the loss
  return loss

@ray.remote([np.ndarray, np.ndarray, np.ndarray], [np.ndarray])
def grad(theta, xs, ys):
  # compute the gradient
  return grad
```

The only difference is that we added the `@ray.remote` decorator specifying a
little bit of type information (the inputs consist of numpy arrays, `loss`
returns a float, and `grad` returns a numpy array).

Now, it is easy to speed up the computation of the full loss and the full
gradient.

```python
def full_loss(theta):
  theta_ref = ray.put(theta)
  loss_refs = [loss(theta_ref, xs_ref, ys_ref) for (xs_ref, ys_ref) in batch_refs]
  return sum([ray.get(loss_ref) for loss_ref in loss_refs])

def full_grad(theta):
  theta_ref = ray.put(theta)
  grad_refs = [grad(theta_ref, xs_ref, ys_ref) for (xs_ref, ys_ref) in batch_refs]
  return sum([ray.get(grad_ref) for grad_ref in grad_refs]).astype("float64") # This conversion is necessary for use with fmin_l_bfgs_b.
```

Note that we turn `theta` into a remote object with the line `theta_ref =
ray.put(theta)` before passing it into the remote functions. If we had written

```python
[loss(theta, xs_ref, ys_ref) for ... in batch_refs]
```

instead of

```python
theta_ref = ray.put(theta)
[loss(theta_ref, xs_ref, ys_ref) for ... in batch_refs]
```

then each task that got sent to the scheduler (one for every element of
`batch_refs`) would have had a copy of `theta` serialized inside of it. Since
`theta` here consists of the parameters of a potentially large model, this is
inefficient. *Large objects should be passed by object reference to remote
functions and not by value*.

We use remote functions and remote objects internally in the implementation of
`full_loss` and `full_grad`, but the user-facing behavior of these methods is
identical to the behavior in the serial version.

We can now optimize the objective with the same function call as before.

```python
theta_init = np.zeros(functions.dim)
result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, fprime=full_grad)
```
