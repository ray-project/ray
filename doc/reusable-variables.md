# Reusable Variables

This document explains how to create and use reusable variables in Ray.

A reusable variable is a per-worker variable which is (1) created when the worker starts, and (2) is reinitialized before a task reuses it. Thus, while a task can modify a reusable variable, the variable is reinitialized before the next task uses it. Reusable variables obviates the need for serialization/deserialization and, like Ray objects, avoid side effects.

Reusable variables are Python objects that are created once on each worker and
can be used by all subsequent tasks that run on that worker. Reusable variables
will be reinitialized between tasks. There are several primary reasons for doing
this.

1. Reusable variables are created once on each worker and are not shipped
between machines, so they do not need to be serialized or deserialized (however,
the code that creates the reusable variable does need to be pickled).
2. Objects that are slow to construct (like a TensorFlow graph) only need to be
constructed once on each worker.
3. By reinitializing between tasks that use them, they help avoid side effects.

To elaborate on the first point, standard Python serialization libraries like
pickle fail on some objects. With these kinds of objects, it may be easier to
ship the code that creates the object to each worker and to run the code on each
worker than it would be to create the object on the driver and ship the object
to each worker.

## Creating a Reusable Variable

To give an example, consider a gym environment, which essentially provides a
Python wrapper for an Atari simulator.

```python
import gym
import ray

ray.init(num_workers=10)

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
