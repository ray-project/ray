Policy Optimizers
=================

RLlib supports using its distributed policy optimizer implementations from external algorithms.

Here are the steps for using a RLlib policy optimizer with an existing algorithm.

1. Implement the `Policy evaluator interface <rllib-dev.html#policy-evaluators-and-optimizers>`__.

    - Here is an example of porting a `PyTorch Rainbow implementation <https://github.com/ericl/Rainbow/blob/rllib-example/rainbow_evaluator.py>`__.

    - Another example porting a `TensorFlow DQN implementation <https://github.com/ericl/baselines/blob/rllib-example/baselines/deepq/dqn_evaluator.py>`__.

2. Pick a `Policy optimizer class <https://github.com/ray-project/ray/tree/master/python/ray/rllib/optimizers>`__. The `LocalSyncOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/local_sync.py>`__ is a reasonable choice for local testing. You can also implement your own. Policy optimizers can be constructed using their ``make`` method (e.g., ``LocalSyncOptimizer.make(evaluator_cls, evaluator_args, num_workers, optimizer_config)``), or you can construct them by passing in a list of evaluators instantiated as Ray actors.

    - Here is code showing the `simple Policy Gradient agent <https://github.com/ray-project/ray/blob/master/python/ray/rllib/pg/pg.py>`__ using ``make()``.

    - A different example showing an `A3C agent <https://github.com/ray-project/ray/blob/master/python/ray/rllib/a3c/a3c.py>`__ passing in Ray actors directly.

3. Decide how you want to drive the training loop.

    - Option 1: call ``optimizer.step()`` from some existing training code. Training statistics can be retrieved by querying the ``optimizer.local_evaluator`` evaluator instance, or mapping over the remote evaluators (e.g., ``ray.get([ev.some_fn.remote() for ev in optimizer.remote_evaluators])``) if you are running with multiple workers.

    - Option 2: define a full RLlib `Agent class <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agent.py>`__. This might be preferable if you don't have an existing training harness or want to use features provided by `Ray Tune <tune.html>`__.


Example of constructing and using a policy optimizer `(link to full example) <https://github.com/ericl/baselines/blob/rllib-example/baselines/deepq/run_simple_loop.py>`__:

.. code-block:: python

    ray.init()
    env_creator = lambda env_config: gym.make("PongNoFrameskip-v4")
    optimizer = LocalSyncReplayOptimizer.make(
        YourEvaluatorClass, [env_creator], num_workers=0, optimizer_config={})

    i = 0
    while optimizer.num_steps_sampled < 100000:
        i += 1
        print("== optimizer step {} ==".format(i))
        optimizer.step()
        print("optimizer stats", optimizer.stats())
        print("local evaluator stats", optimizer.local_evaluator.stats())

Available Policy Optimizers
---------------------------

+-----------------------------+---------------------+-----------------+------------------------------+
| **Policy optimizer class**  | **Operating range** | **Works with**  | **Description**              |
+=============================+=====================+=================+==============================+
|AsyncOptimizer               |1-10s of CPUs        |(any)            |Asynchronous gradient-based   |
|                             |                     |                 |optimization (e.g., A3C)      |
+-----------------------------+---------------------+-----------------+------------------------------+
|LocalSyncOptimizer           |0-1 GPUs +           |(any)            |Synchronous gradient-based    |
|                             |1-100s of CPUs       |                 |optimization with parallel    |
|                             |                     |                 |sample collection             |
+-----------------------------+---------------------+-----------------+------------------------------+
|LocalSyncReplayOptimizer     |0-1 GPUs +           | Off-policy      |Adds a replay buffer          |
|                             |1-100s of CPUs       | algorithms      |to LocalSyncOptimizer         |
+-----------------------------+---------------------+-----------------+------------------------------+
|LocalMultiGPUOptimizer       |0-10 GPUs +          | Algorithms      |Implements data-parallel      |
|                             |1-100s of CPUs       | written in      |optimization over multiple    |
|                             |                     | TensorFlow      |GPUs, e.g., for PPO           |
+-----------------------------+---------------------+-----------------+------------------------------+
|ApexOptimizer                |1 GPU +              | Off-policy      |Implements the Ape-X          |
|                             |10-100s of CPUs      | algorithms      |distributed prioritization    |
|                             |                     | w/sample        |algorithm                     |
|                             |                     | prioritization  |                              |
+-----------------------------+---------------------+-----------------+------------------------------+
