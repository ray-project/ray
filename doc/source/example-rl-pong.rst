Learning to Play Pong
=====================

In this example, we'll be training a neural network to play Pong using the
OpenAI Gym. This application is adapted, with minimal modifications, from Andrej
Karpathy's
[code](https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5) (see
the accompanying [blog post](http://karpathy.github.io/2016/05/31/rl/)). To run
the application, first install some dependencies.

.. code-block:: bash

  pip install gym[atari]

Then you can run the example as follows.

.. code-block:: bash

  python ray/examples/rl_pong/driver.py

The distributed version
-----------------------

At the core of [Andrej's
code](https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5), a
neural network is used to define a "policy" for playing Pong (that is, a
function that chooses an action given a state). In the loop, the network
repeatedly plays games of Pong and records a gradient from each game. Every ten
games, the gradients are combined together and used to update the network.

This example is easy to parallelize because the network can play ten games in
parallel and no information needs to be shared between the games.

We define an **actor** for the Pong environment, which includes a method for
performing a rollout and computing a gradient update. Below is pseudocode for
the actor.

.. code-block:: python

  @ray.actor
  class PongEnv(object):
    def __init__(self):
      self.env = gym.make("Pong-v0")

    def compute_gradient(self, model):
      # Reset the game.
      observation = self.env.reset()
      while not done:
        # Choose an action using policy_forward.
        # Take the action and observe the new state of the world.
      # Compute a gradient using policy_backward. Return the gradient and reward.
      return [gradient, reward_sum]

We then create a number of actors, so that we can perform rollouts in parallel.

.. code-block:: python

  actors = [PongEnv() for _ in range(batch_size)]

Calling this remote function inside of a for loop, we launch multiple tasks to
perform rollouts and compute gradients in parallel.

.. code-block:: python

  model_id = ray.put(model)
  actions = []
  # Launch tasks to compute gradients from multiple rollouts in parallel.
  for i in range(batch_size):
    action_id = actors[i].compute_gradient(model_id)
    actions.append(action_id)
