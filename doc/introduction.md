## Introduction

The goal of Ray is to make it easy to write machine learning applications that
run on a cluster while providing the development and debugging experience of
working on a single machine.

Before jumping into the details, here's a simple Python example for doing a
Monte Carlo estimation of pi (using multiple cores or potentially multiple
machines).
```python
import ray
import functions # See definition below

results = []
for _ in range(10):
  results.append(functions.estimate_pi(100))
estimate = np.mean([ray.get(ref) for ref in results])
print "Pi is approximately {}.".format(estimate)
```

This assumes that we've defined the file `functions.py` as below.
```python
import ray
import numpy as np

@ray.remote([int], [float])
def estimate_pi(n):
  x = np.random.uniform(size=n)
  y = np.random.uniform(size=n)
  return 4 * np.mean(x ** 2 + y ** 2 < 1)
```

Within the for loop, each call to `functions.estimate_pi(100)` sends a message
to the scheduler asking it to schedule the task of running
`functions.estimate_pi` with the argument `100`. This call returns right away
without waiting for the actual estimation of pi to take place. Instead of
returning a float, it returns an **object reference**, which represents the
eventual output of the computation.

The call to `ray.get(ref)` takes an object reference and returns the actual
estimate of pi (waiting until the computation has finished if necessary).

## Next Steps

- [Download and Setup](download-and-setup.md)
- [Basic Usage](basic-usage.md)
- [Tutorial](tutorial.md)
- [About the System](about-the-system.md)
- [Using Ray on a Cluster](using-ray-on-a-cluster.md)

## Example Applications

- [Hyperparameter Optimization](../examples/hyperopt/README.md)
- [Batch L-BFGS](../examples/lbfgs/README.md)
