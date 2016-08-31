# Learning to Play Pong

In this example, we'll be training a neural network to play Pong using the
OpenAI Gym. This application is adapted, with minimal modifications, from Andrej
Karpathy's
[code](https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5) (see
the accompanying [blog post](http://karpathy.github.io/2016/05/31/rl/)). To run
the application, first install this dependency.

- [Gym](https://gym.openai.com/)

Then from the directory `ray/examples/rl_pong/` run the following.

```
source ../../setup-env.sh
python driver.py
```

## The distributed version

At the core of [Andrej's
code](https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5), a
neural network is used to define a "policy" for playing Pong (that is, a
function that chooses an action given a state). In the loop, the network
repeatedly plays games of Pong and records a gradient from each game. Every ten
games, the gradients are combined together and used to update the network.

This example is easy to parallelize because the network can play ten games in
parallel and no information needs to be shared between the games. We define a
remote function `compute_gradient`, which plays a game of pong and returns an
estimate of the gradient. Below is a simplified pseudocode version of this
function.

```python
@ray.remote(num_return_vals=2)
def compute_gradient(model):
  # Retrieve the game environment.
  env = ray.reusables.env
  # Reset the game.
  observation = env.reset()
  while not done:
    # Choose an action using policy_forward.
    # Take the action and observe the new state of the world.
  # Compute a gradient using policy_backward. Return the gradient and reward.
  return gradient, reward_sum
```

Calling this remote function inside of a for loop, we launch multiple tasks to
perform rollouts and compute gradients. If we have at least ten worker
processes, then these tasks will all be executed in parallel.

```python
model_id = ray.put(model)
grads, reward_sums = [], []
# Launch tasks to compute gradients from multiple rollouts in parallel.
for i in range(10):
  grad_id, reward_sum_id = compute_gradient.remote(model_id)
  grads.append(grad_id)
  reward_sums.append(reward_sum_id)
```

### Reusing the Gym environment

Workers are long-running Python processes, and though we'd like to think of
workers as being stateless, sometimes it's important to have a variable that
gets shared between different tasks on the same worker (perhaps because it is
expensive to initialize the variable).

In this example, we'd like each worker to have access to a Pong environment. The
Pong environment has state that gets mutated by the task, and this state is
shared between tasks that run on the same worker, so there is some danger that
the output of the overall program will depend on which tasks are scheduled on
which workers. This can be avoided if the state of the Pong environment is reset
between tasks.

To accomplish this, the user must mark the Pong environment as a reusable
variable. This is done by providing a method for initializing the gym, and
storing it in `ray.reusables`.

```python
# Function for initializing the gym environment.
def env_initializer():
  return gym.make("Pong-v0")

# Create a reusable variable for the gym environment.
ray.reusables.env = ray.Reusable(env_initializer)
```

A remote task can then call `ray.reusables.env` to retrieve the variable.

By default, whenever a task uses the `ray.reusables.env` variable, the worker
that the task was scheduled on will rerun the initialization code
`env_initializer` after the task has finished so that state will not leak
between the tasks.

However, sometimes the initialization code is expensive, and there may be a
faster way to reinitialize the variable (or maybe no reinitialization is needed
at all). In these cases, the user can provide a custom **reinitializer**, which
gets run after any task that uses the variable.

```python
# Function for initializing the gym environment.
def env_initializer():
  return gym.make("Pong-v0")

# Function for reinitializing the gym environment in order to guarantee that
# the state of the game is reset after each remote task.
def env_reinitializer(env):
  env.reset()
  return env

# Create a reusable variable for the gym environment.
ray.reusables.env = ray.Reusable(env_initializer, env_reinitializer)
```
