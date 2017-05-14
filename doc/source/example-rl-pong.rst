Learning to Play Pong
=====================

In this example, we'll train a **very simple** neural network to play Pong using
the OpenAI Gym. This application is adapted, with minimal modifications, from
Andrej Karpathy's `code`_ (see the accompanying `blog post`_).

You can view the `code for this example`_.

To run the application, first install some dependencies.

.. code-block:: bash

  pip install gym[atari]

Then you can run the example as follows.

.. code-block:: bash

  python ray/examples/rl_pong/driver.py --batch-size=10

To run the example on a cluster, simple pass in the flag
``--redis-address=<redis-address>``.

At the moment, on a large machine with 64 physical cores, computing an update
with a batch of size 1 takes about 1 second, a batch of size 10 takes about 2.5
seconds. A batch of size 60 takes about 3 seconds. On a cluster with 11 nodes,
each with 18 physical cores, a batch of size 300 takes about 10 seconds. If the
numbers you see differ from these by much, take a look at the
**Troubleshooting** section at the bottom of this page and consider `submitting
an issue`_.

.. _`code`: https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5
.. _`blog post`: http://karpathy.github.io/2016/05/31/rl/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/rl_pong
.. _`submitting an issue`: https://github.com/ray-project/ray/issues

**Note** that these times depend on how long the rollouts take, which in turn
depends on how well the policy is doing. For example, a really bad policy will
lose very quickly. As the policy learns, we should expect these numbers to
increase.

The distributed version
-----------------------

At the core of Andrej's `code`_, a neural network is used to define a "policy"
for playing Pong (that is, a function that chooses an action given a state). In
the loop, the network repeatedly plays games of Pong and records a gradient from
each game. Every ten games, the gradients are combined together and used to
update the network.

This example is easy to parallelize because the network can play ten games in
parallel and no information needs to be shared between the games.

We define an **actor** for the Pong environment, which includes a method for
performing a rollout and computing a gradient update. Below is pseudocode for
the actor.

.. code-block:: python

  @ray.remote
  class PongEnv(object):
    def __init__(self):
      # Tell numpy to only use one core. If we don't do this, each actor may try
      # to use all of the cores and the resulting contention may result in no
      # speedup over the serial version. Note that if numpy is using OpenBLAS,
      # then you need to set OPENBLAS_NUM_THREADS=1, and you probably need to do
      # it from the command line (so it happens before numpy is imported).
      os.environ["MKL_NUM_THREADS"] = "1"
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
    action_id = actors[i].compute_gradient.remote(model_id)
    actions.append(action_id)


Troubleshooting
---------------

If you are not seeing any speedup from Ray (and assuming you're using a
multicore machine), the problem may be that numpy is trying to use multiple
threads. When many processes are each trying to use multiple threads, the result
is often no speedup. When running this example, try opening up ``top`` and
seeing if some python processes are using more than 100% CPU. If yes, then this
is likely the problem.

The example tries to set ``MKL_NUM_THREADS=1`` in the actor. However, that only
works if the numpy on your machine is actually using MKL. If it's using
OpenBLAS, then you'll need to set ``OPENBLAS_NUM_THREADS=1``. In fact, you may
have to do this **before** running the script (it may need to happen before
numpy is imported).

.. code-block:: python

  export OPENBLAS_NUM_THREADS=1
