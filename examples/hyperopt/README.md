## Hyperparameter Optimization

This document provides a walkthrough of the hyperparameter optimization example.
To run the application, first install this dependency.

- [TensorFlow](https://www.tensorflow.org/)

Then from the directory `ray/examples/hyperopt/` run the following.

```
source ../../setup-env.sh
python driver.py # This will take a minute to first download the MNIST dataset.
```

Machine learning algorithms often have a number of *hyperparameters* whose
values must be chosen by the practitioner. For example, an optimization
algorithm may have a step size, a decay rate, and a regularization coefficient.
In a deep network, the network parameterization itself (e.g., the number of
layers and the number of units per layer) can be considered a hyperparameter.

Choosing these parameters can be challenging, and so a common practice is to
search over the space of hyperparameters. One approach that works surprisingly
well is to randomly sample different options.

### The serial version

Suppose that we want to train a convolutional network, but we aren't sure how to
choose the following hyperparameters:

- the learning rate
- the batch size
- the dropout probability
- the standard deviation of the distribution from which to initialize the
network weights

Suppose that we've defined a Python function `train_cnn_and_compute_accuracy`,
which takes values for these hyperparameters as its input (along with the
dataset), trains a convolutional network using those hyperparameters, and
returns the accuracy of the trained model on a validation set.

```python
def train_cnn_and_compute_accuracy(hyperparameters, train_images, train_labels, validation_images, validation_labels):
  # Construct a deep network, train it, and return the validation accuracy.
  # The argument hyperparameters is a dictionary with keys:
  #   - "learning_rate"
  #   - "batch_size"
  #   - "dropout"
  #   - "stddev"
  return validation_accuracy
```

Something that works surprisingly well is to try random values for the
hyperparameters. For example, we can write the following.

```python
def generate_random_params():
  # Randomly choose values for the hyperparameters
  learning_rate = 10 ** np.random.uniform(-5, 5)
  batch_size = np.random.randint(1, 100)
  dropout = np.random.uniform(0, 1)
  stddev = 10 ** np.random.uniform(-5, 5)
  return {"learning_rate": learning_rate, "batch_size": batch_size, "dropout": dropout, "stddev": stddev}

results = []
for _ in range(100):
  randparams = generate_random_params()
  results.append((randparams, train_cnn_and_compute_accuracy(randparams, epochs)))
```

Then we can inspect the contents of `results` and see which set of
hyperparameters worked the best.

Of course, as there are no dependencies between the different invocations of
`train_cnn_and_compute_accuracy`, this computation could easily be parallelized
over multiple cores or multiple machines. Let's do that now.

### The distributed version

First, let's turn `train_cnn_and_compute_accuracy` into a remote function in Ray
by writing it as follows. In this example application, a slightly more
complicated version of this remote function is defined in
[hyperopt.py](hyperopt.py).

```python
@ray.remote([dict, np.ndarray, np.ndarray, np.ndarray, np.ndarray], [float])
def train_cnn_and_compute_accuracy(hyperparameters, train_images, train_labels, validation_images, validation_labels):
  # Actual work omitted.
  return validation_accuracy
```

The only difference is that we added the `@ray.remote` decorator specifying a
little bit of type information (the input is a dictionary along with some numpy
arrays, and the return value is a float).

Now a call to `train_cnn_and_compute_accuracy` does not execute the function. It
submits the task to the scheduler and returns an object reference for the output
of the eventual computation. The scheduler, at its leisure, will schedule the
task on a worker (which may live on the same machine or on a different machine
in the cluster).

Now the for loop runs almost instantaneously because it does not do any actual
computation. Instead, it simply submits a number of tasks to the scheduler.

```python
result_refs = []
for _ in range(100):
  params = generate_random_params()
  results.append((params, train_cnn_and_compute_accuracy(params, epochs)))
```

If we wish to wait until the results have all been retrieved, we can retrieve
their values with `ray.get`.

```python
results = [(params, ray.get(ref)) for (params, ref) in result_refs]
```

### Additional notes

**Early Stopping:** Sometimes when running an optimization, it is clear early on
that the hyperparameters being used are bad (for example, the loss function may
start diverging). In these situations, it makes sense to end that particular run
early to save resources. This is implemented within the remote function
`train_cnn_and_compute_accuracy`. If it detects that the optimization is going
poorly, it returns early.
