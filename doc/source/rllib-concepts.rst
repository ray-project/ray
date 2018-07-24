RLlib Concepts
==============

.. note::

    To learn more about these concepts, see also the `ICML paper <https://arxiv.org/abs/1712.09381>`__.

Policy Graphs
-------------

Policy graph classes encapsulate the core numerical components of RL algorithms. This typically includes the policy model that determines actions to take, a trajectory postprocessor for experiences, and a loss function to improve the policy given postprocessed experiences. For a simple example, see the policy gradients `graph definition <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/pg/pg_policy_graph.py>`__.

Most interaction with deep learning frameworks is isolated to the `PolicyGraph interface <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/policy_graph.py>`__, allowing RLlib to support multiple frameworks. To simplify the definition of policy graphs, RLlib includes `Tensorflow <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/tf_policy_graph.py>`__ and `PyTorch-specific <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/torch_policy_graph.py>`__ templates.

Policy Evaluation
-----------------

Given an environment and policy graph, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `PolicyEvaluator <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/policy_evaluator.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can also use policy evaluation standalone to produce batches of experiences. This can be done by calling ``ev.sample()`` on an evaluator instance, or ``ev.sample.remote()`` in parallel on evaluator instances created as Ray actors (see ``PolicyEvalutor.as_remote()``).

Policy Optimization
-------------------

Similar to how a `gradient-descent optimizer <https://www.tensorflow.org/api_docs/python/tf/train/GradientDescentOptimizer>`__ can be used to improve a model, RLlib's `policy optimizers <https://github.com/ray-project/ray/tree/master/python/ray/rllib/optimizers>`__ implement different strategies for improving a policy graph.

For example, in A3C you'd want to compute gradients asynchronously on different workers, and apply them to a central policy graph replica. This strategy is implemented by the `AsyncGradientsOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/async_gradients_optimizer.py>`__. Another alternative is to gather experiences synchronously in parallel and optimize the model centrally, as in `SyncSamplesOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/sync_samples_optimizer.py>`__. Policy optimizers abstract these strategies away into reusable modules.
