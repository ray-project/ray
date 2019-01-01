RLlib Offline Data Input / Output
=================================

.. note::

    RLlib I/O is currently *experimental*. Please report any `issues <https://github.com/ray-project/ray/issues>`__ you encounter.

Working with Offline Experiences
--------------------------------

RLlib's I/O APIs enable you to work with datasets of experiences read from offline storage (e.g., disk, cloud storage, streaming systems, HDFS). For example, you might want to read experiences saved from previous simulation runs, or gathered from policies deployed in `web applications <https://arxiv.org/abs/1811.00260>`__. You can also log new agent experiences produced during training for future use.

RLlib represents trajectory sequences (i.e., ``(s, a, r, s')`` tuples) with `SampleBatch <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/sample_batch.py>`__ objects. Using a batch format enables efficient encoding and compression of experiences during training. During online training, RLlib uses `policy evaluation <rllib-concepts.html#policy-evaluation>`__ actors to generate batches of experiences in parallel using the current policy. RLlib also uses this same batch format for reading and writing experiences to offline storage.

Example: Training on previously saved experiences
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In this example, we will save batches of experiences to disk, and then leverage those batches to train a policy using DQN. First, we run a simple policy gradient algorithm for 100k steps with ``"output": "/tmp/cartpole-out"`` to tell RLlib to write simulation outputs to the ``/tmp/cartpole-out`` directory.

.. code-block:: bash

    $ rllib train
        --run=PG \
        --env=CartPole-v0 \
        --config='{"output": "/tmp/cartpole-out", "output_max_file_size": 5000000}' \
        --stop='{"timesteps_total": 100000}'

The experiences will be saved in compressed JSON batch format:

.. code-block:: text

    $ ls -l /tmp/cartpole-out
    total 11636
    -rw-rw-r-- 1 eric eric 5022257 output-2019-01-01_15-58-57_worker-0_0.json
    -rw-rw-r-- 1 eric eric 5002416 output-2019-01-01_15-59-22_worker-0_1.json
    -rw-rw-r-- 1 eric eric 1881666 output-2019-01-01_15-59-47_worker-0_2.json

Then, we can tell DQN to train using these previously generated experiences with ``"input": "/tmp/cartpole-out"``:

.. code-block:: bash

    $ rllib train \
        --run=DQN \
        --env=CartPole-v0 \
        --config='{
            "input": "/tmp/cartpole-out",
            "exploration_final_eps": 0,
            "schedule_max_timesteps": 10}'

Since the input experiences are not from running simulations, RLlib cannot report the true policy performance. However, you can use ``tensorboard --logdir=~/ray-results`` to monitor training progress via other metrics such as estimated Q-value:

.. image:: offline-q.png

In this input mode, no simulations are run, though you still need to specify the environment in order to define the action and observation spaces. If true simulation is also possible (i.e., your env supports ``step()``), you can also set ``"input_evaluation": "simulation"`` to additionally run simulations to estimate the true policy performance. The output of these simulations will not be used for learning.

Example: Converting experiences to batch format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a non-simulated application, it will is necessary to generate the ``*.json`` experience batch files outside of RLlib. This can be done by using the `JsonWriter <https://github.com/ray-project/ray/blob/master/python/ray/rllib/offline/json_writer.py>`__ class:

To address this, the policy needs to be re-deployed to gather new experiences.

Unless your experience dataset adequately covers the space of possible states and actions (as it is in the toy cartpole example), your policy will not converge to an optimal solution.

On-policy algorithms and experience postprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example: Mixing simulation and offline data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unless your experience dataset adequately covers the space of possible states and actions (as it is in the toy cartpole example), your policy will not converge to an optimal solution. To address this, the policy needs to be re-deployed to gather new experiences. In this example we show how this can be done when a simulator is available:

Step 1: 

.. code-block:: bash

    $ rllib train.py \
        --run=DQN \
        --env=CartPole-v0 \
        --config='{"input": "/tmp/cartpole-out"}' \
        --checkpoint-freq=1


Generating Experiences: Online
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sampler -> postprocessing(batch) -> gradients(theta, batch)

Input API
---------

Output API
----------

