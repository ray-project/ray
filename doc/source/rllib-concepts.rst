RLlib Concepts
==============

This page describes the internal concepts used to implement algorithms in RLlib. You might find this useful if modifying or adding new algorithms to RLlib.

Policy Graphs
-------------

Policy graph classes encapsulate the core numerical components of RL algorithms. This typically includes the policy model that determines actions to take, a trajectory postprocessor for experiences, and a loss function to improve the policy given postprocessed experiences. For a simple example, see the policy gradients `graph definition <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/pg/pg_policy_graph.py>`__.

Most interaction with deep learning frameworks is isolated to the `PolicyGraph interface <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/policy_graph.py>`__, allowing RLlib to support multiple frameworks. To simplify the definition of policy graphs, RLlib includes `Tensorflow <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/tf_policy_graph.py>`__ and `PyTorch-specific <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/torch_policy_graph.py>`__ templates. You can also write your own from scratch. Here is an example:

.. code-block:: python

    class CustomPolicy(PolicyGraph):
        """Example of a custom policy graph written from scratch.

        You might find it more convenient to extend TF/TorchPolicyGraph instead
        for a real policy.
        """

        def __init__(self, observation_space, action_space, config):
            PolicyGraph.__init__(self, observation_space, action_space, config)
            # example parameter
            self.w = 1.0

        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):
            # return action batch, RNN states, extra values to include in batch
            return [self.action_space.sample() for _ in obs_batch], [], {}

        def learn_on_batch(self, samples):
            # implement your learning code here
            return {}  # return stats

        def get_weights(self):
            return {"w": self.w}

        def set_weights(self, weights):
            self.w = weights["w"]

Policy Evaluation
-----------------

Given an environment and policy graph, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `PolicyEvaluator <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/policy_evaluator.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can use policy evaluation standalone to produce batches of experiences. This can be done by calling ``ev.sample()`` on an evaluator instance, or ``ev.sample.remote()`` in parallel on evaluator instances created as Ray actors (see ``PolicyEvaluator.as_remote()``).

Here is an example of creating a set of policy evaluation actors and using the to gather experiences in parallel. The trajectories are concatenated, the policy learns on the trajectory batch, and then we broadcast the policy weights to the evaluators for the next round of rollouts:

.. code-block:: python

    # Setup policy and remote policy evaluation actors
    env = gym.make("CartPole-v0")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    remote_evaluators = [
        PolicyEvaluator.as_remote().remote(lambda c: gym.make("CartPole-v0"),
                                           CustomPolicy)
        for _ in range(10)
    ]

    while True:
        # Gather a batch of samples
        T1 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in remote_evaluators]))

        # Improve the policy using the T1 batch
        policy.learn_on_batch(T1)

        # Broadcast weights to the policy evaluation workers
        weights = ray.put({"default_policy": policy.get_weights()})
        for w in remote_evaluators:
            w.set_weights.remote(weights)

Policy Optimization
-------------------

Similar to how a `gradient-descent optimizer <https://www.tensorflow.org/api_docs/python/tf/train/GradientDescentOptimizer>`__ can be used to improve a model, RLlib's `policy optimizers <https://github.com/ray-project/ray/tree/master/python/ray/rllib/optimizers>`__ implement different strategies for improving a policy graph.

For example, in A3C you'd want to compute gradients asynchronously on different workers, and apply them to a central policy graph replica. This strategy is implemented by the `AsyncGradientsOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/async_gradients_optimizer.py>`__. Another alternative is to gather experiences synchronously in parallel and optimize the model centrally, as in `SyncSamplesOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/sync_samples_optimizer.py>`__. Policy optimizers abstract these strategies away into reusable modules.

This is how the example in the previous section looks when written using a policy optimizer:

.. code-block:: python

    # Same setup as before
    local_evaluator = PolicyEvaluator(lambda c: gym.make("CartPole-v0"), CustomPolicy)
    remote_evaluators = [
        PolicyEvaluator.as_remote().remote(lambda c: gym.make("CartPole-v0"),
                                           CustomPolicy)
        for _ in range(10)
    ]
    
    # this optimizer implements the IMPALA architecture
    optimizer = AsyncSamplesOptimizer(
        local_evaluator, remote_evaluators, train_batch_size=500)

    while True:
        optimizer.step()


Trainers
--------

Trainers are the boilerplate classes that put the above components together, making algorithms accessible via Python API and the command line. They manage algorithm configuration, setup of the policy evaluators and optimizer, and collection of training metrics. Trainers also implement the `Trainable API <https://ray.readthedocs.io/en/latest/tune-usage.html#training-api>`__ for easy experiment management.

Example of two equivalent ways of interacting with the PPO trainer:

.. code-block:: python

    trainer = PPOTrainer(env="CartPole-v0", config={"train_batch_size": 4000})
    while True:
        print(trainer.train())

.. code-block:: bash

    rllib train --run=PPO --env=CartPole-v0 --config='{"train_batch_size": 4000}'
