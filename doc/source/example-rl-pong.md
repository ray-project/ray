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
function`compute_gradient` that is defined in a Ray actor, which plays a game 
of pong and returns an estimate of the gradient. Below is a simplified 
pseudocode version of this function.

```python
def compute_gradient(model):
  # Retrieve the game environment.
  env = ray.env.env
  # Reset the game.
  observation = env.reset()
  while not done:
    # Choose an action using policy_forward.
    # Take the action and observe the new state of the world.
  # Compute a gradient using policy_backward. Return the gradient and reward.
  return [gradient, reward_sum]
```

Calling this remote function inside of a for loop, we launch multiple tasks to
perform rollouts and compute gradients in parallel.

```python
model_id = ray.put(model)
actions = []
# Launch tasks to compute gradients from multiple rollouts in parallel.
for i in range(batch_size):
  action_id = actors[i].compute_gradient(model_id)
  actions.append(action_id)
```

### Reusing the Gym environment

In this example, we'd like to have remote functions that will have access to a Pong 
environment, and will always properly reset that environment. To accomplish this, 
the user must embed the Pong environment into a Ray actor, and implement their methods 
as methods of the actor. As each actor has a specific environment, we do not have to 
worry about using environments that were already touched by other processes.

```python
@ray.actor
class PongEnv(object):
  def __init__(self):
    self.env = gym.make("Pong-v0")
```
