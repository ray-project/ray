# Hyperparameter Optimization

This document provides a walkthrough of the hyperparameter optimization example.
To run the application, first install this dependency.

- [TensorFlow](https://www.tensorflow.org/)

Then from the directory `ray/examples/hyperopt/` run the following.

```
source ../../setup-env.sh
python driver.py
```

Machine learning algorithms often have a number of *hyperparameters* whose
values must be chosen by the practitioner. For example, an optimization
algorithm may have a step size, a decay rate, and a regularization coefficient.
In a deep network, the network parameterization itself (e.g., the number of
layers and the number of units per layer) can be considered a hyperparameter.

Choosing these parameters can be challenging, and so a common practice is to
search over the space of hyperparameters. One approach that works surprisingly
well is to randomly sample different options.

## The serial version

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
  params = generate_random_params()
  accuracy = train_cnn_and_compute_accuracy(randparams, train_images, train_labels, validation_images, validation_labels)
  results.append(accuracy)
```

Then we can inspect the contents of `results` and see which set of
hyperparameters worked the best.

Of course, as there are no dependencies between the different invocations of
`train_cnn_and_compute_accuracy`, this computation could easily be parallelized
over multiple cores or multiple machines. Let's do that now.

## The distributed version

First, let's turn `train_cnn_and_compute_accuracy` into a remote function in Ray
by writing it as follows. In this example application, a slightly more
complicated version of this remote function is defined in
[hyperopt.py](hyperopt.py).

```python
@ray.remote
def train_cnn_and_compute_accuracy(hyperparameters, train_images, train_labels, validation_images, validation_labels):
  # Actual work omitted.
  return validation_accuracy
```

The only difference is that we added the `@ray.remote` decorator.

Now a call to `train_cnn_and_compute_accuracy` does not execute the function. It
submits the task to the scheduler and returns an object ID for the output
of the eventual computation. The scheduler, at its leisure, will schedule the
task on a worker (which may live on the same machine or on a different machine
in the cluster).

Now the for loop runs almost instantaneously because it does not do any actual
computation. Instead, it simply submits a number of tasks to the scheduler.

```python
result_ids = []
# Launch 100 tasks.
for _ in range(100):
  params = generate_random_params()
  accuracy_id = train_cnn_and_compute_accuracy.remote(randparams, train_images, train_labels, validation_images, validation_labels)
  result_ids.append(accuracy_id)
```

If we wish to wait until the results have all been retrieved, we can retrieve
their values with `ray.get`.

```python
results = ray.get(result_ids)
```

One drawback of the above approach is that nothing will be printed until all of
the experiments have finished. What we'd really like is to start processing
the results of certain experiments as soon as they finish (and possibly launch
more experiments based on the outcomes of the first ones). To do this, we can
use `ray.wait`, which takes a list of object IDs and returns two lists of object
IDs.

```python
ready_ids, remaining_ids = ray.wait(result_ids, num_returns=3, timeout=10)
```

In the above, `result_ids` is a list of object IDs. The command `ray.wait` will
return as soon as either three of the object IDs in `result_ids` are ready (that
is, the task that created the corresponding object finished executing and stored
the object in the object store) or ten seconds pass, whichever comes first. To
wait indefinitely, omit the timeout argument. Now, we can rewrite the script as
follows.

```python
remaining_ids = []
# Launch 100 tasks.
for _ in range(100):
  params = generate_random_params()
  accuracy_id = train_cnn_and_compute_accuracy.remote(randparams, train_images, train_labels, validation_images, validation_labels)
  result_ids.append(accuracy_id)

# Process the tasks one at a time.
while len(remaining_ids) > 0:
  # Process the next task that finishes.
  ready_ids, remaining_ids = ray.wait(remaining_ids, num_returns=1)
  # Get the accuracy corresponding to the ready object ID.
  accuracy = ray.get(ready_ids[0])
  print "Accuracy {}".format(accuracy)
```

## Additional notes

**Early Stopping:** Sometimes when running an optimization, it is clear early on
that the hyperparameters being used are bad (for example, the loss function may
start diverging). In these situations, it makes sense to end that particular run
early to save resources. This is implemented within the remote function
`train_cnn_and_compute_accuracy`. If it detects that the optimization is going
poorly, it returns early.
