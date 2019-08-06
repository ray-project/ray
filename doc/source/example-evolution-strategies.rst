Evolution Strategies
====================

This document provides a walkthrough of the evolution strategies example.
To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow
  pip install gym

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/rllib/agents/es

The script can be run as follows. Note that the configuration is tuned to work
on the ``Humanoid-v1`` gym environment.

.. code-block:: bash

  rllib train --env=Humanoid-v1 --run=ES

To train a policy on a cluster (e.g., using 900 workers), run the following.

.. code-block:: bash

  rllib train \
      --env=Humanoid-v1 \
      --run=ES \
      --redis-address=<redis-address> \
      --config='{"num_workers": 900, "episodes_per_batch": 10000, "train_batch_size": 100000}'

At the heart of this example, we define a ``Worker`` class. These workers have
a method ``do_rollouts``, which will be used to perform simulate randomly
perturbed policies in a given environment.

.. code-block:: python

  @ray.remote
  class Worker(object):
      def __init__(self, config, policy_params, env_name, noise):
          self.env = # Initialize environment.
          self.policy = # Construct policy.
          # Details omitted.

      def do_rollouts(self, params):
          perturbation = # Generate a random perturbation to the policy.

          self.policy.set_weights(params + perturbation)
          # Do rollout with the perturbed policy.

          self.policy.set_weights(params - perturbation)
          # Do rollout with the perturbed policy.

          # Return the rewards.

In the main loop, we create a number of actors with this class.

.. code-block:: python

  workers = [Worker.remote(config, policy_params, env_name, noise_id)
             for _ in range(num_workers)]

We then enter an infinite loop in which we use the actors to perform rollouts
and use the rewards from the rollouts to update the policy.

.. code-block:: python

  while True:
      # Get the current policy weights.
      theta = policy.get_weights()
      # Put the current policy weights in the object store.
      theta_id = ray.put(theta)
      # Use the actors to do rollouts, note that we pass in the ID of the policy
      # weights.
      rollout_ids = [worker.do_rollouts.remote(theta_id), for worker in workers]
      # Get the results of the rollouts.
      results = ray.get(rollout_ids)
      # Update the policy.
      optimizer.update(...)

In addition, note that we create a large object representing a shared block of
random noise. We then put the block in the object store so that each ``Worker``
actor can use it without creating its own copy.

.. code-block:: python

  @ray.remote
  def create_shared_noise():
      noise = np.random.randn(250000000)
      return noise

  noise_id = create_shared_noise.remote()

Recall that the ``noise_id`` argument is passed into the actor constructor.
