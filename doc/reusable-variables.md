# Reusable Variables

This document explains how to create and use reusable variables in Ray.

Reusable variables are Python objects that are created once on each worker and
are reused by all subsequent tasks that run on that worker. There are two
primary reasons for doing this.

1. You want to use an object that is not serializable, and so the code that
creates the object must run on each worker.
2. Creating the object is slow (like a TensorFlow graph), and so you want to
create it only once.

To elaborate on the first point, certain Python objects cannot be easily shipped
between processes or machines. Certain objects simply are not meaningful on
other machines (such as a filehandle). For others, their classes may be largely
defined in C, and so their internals may be relatively opaque to Python.

In these situations, if we need the object on all of the workers, then each
worker needs to run the code that creates the object. We accomplish this using
reusable variables. These variables are created once on each worker and are
reused by every task that runs on that worker.

## Creating a Reusable Variable

To give an example, consider a gym environment, which is essentially provides a
Python wrapper for an Atari simulator.

```python
import gym
import ray

ray.init(start_ray_local=True, num_workers=5)

# Define a function to create the gym environment.
def env_initializer():
  return gym.make("Pong-v0")

# Create the reusable variable. This line will cause env_initializer to run on
# each worker and on the driver.
ray.reusables.env = ray.Reusable(env_initializer)

# Define a remote function that uses the gym environment.
@ray.remote
def step():
  env = ray.reusables.env
  # Choose a random action.
  action = env.action_space.sample()
  # Take the action and return the result.
  return env.step(action)

# Call the remote function.
step.remote()
```

When the gym is created, it prints something like `Making new env: Pong-v0`. You
may notice that this is printed once for each worker. Calling `step.remote()`
will run a remote function that uses the `env` variable. You may notice that
calling `step.remote()` causes the line `Making new env: Pong-v0` to be printed
again. That occurs because, by default, every time a remote function uses a
reusable variable, the worker will rerun the code that initializes the reusable
variable to prevent side effects from leaking between tasks and introducing
non-determinism into the program.

Of course, rerunning the initialization code can be expensive, so a custom
reinitializer can be passed into the creation of a reusable variable. If the
state of the reusable variable is not mutated by any remote function, then the
reinitialization code can just be the identity function.

```python
# Define a function to create the gym environment.
def env_initializer():
  return gym.make("Pong-v0")

# Define a function to reinitialize the gym environment.
def env_reinitializer(env):
  env.reset()
  return env

# Create the reusable variable. This line will cause env_initializer to run on
# each worker and on the driver. Every time a remote function uses the reusable
# variable, env_reinitializer will run to reset the state of the variable.
ray.reusables.env = ray.Reusable(env_initializer, env_reinitializer)

# Define a remote function that uses the gym environment.
@ray.remote
def step():
  env = ray.reusables.env
  # Choose a random action.
  action = env.action_space.sample()
  # Take the action and return the result.
  return env.step(action)

# Call the remote function.
step.remote()
```

**Note:** It may sometimes look like Ray is hanging and not responding. This can
occur when print statements happen in the background on workers and hide the
interpreter prompt. Try pressing enter, and see if that fixes it.
