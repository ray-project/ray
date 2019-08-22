
The distributed version
-----------------------

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
