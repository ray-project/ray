.. include:: we_are_hiring.rst

.. _rllib-index:

RLlib: Industry-Grade Reinforcement Learning
============================================

.. figure:: ../images/rllib/rllib-index-header.svg

**RLlib** is an open-source library for reinforcement learning (RL), offering support for
production-level, highly distributed RL workloads while maintaining
unified and simple APIs for a large variety of industry applications.
Whether you would like to train your agents in a multi-agent setup,
purely from offline (historic) datasets, or using externally
connected simulators, RLlib offers a simple solution for each of your decision
making needs.

You **don't need** to be an **RL expert**, nor do you need to
learn Ray or any of its other libraries in order to get started with RLlib.
If you either have your problem defined and coded in python as an
"`RL environment <https://github.com/openai/gym>`_" or are in possession of pre-recorded
(historic) data to learn from, you should be up and running with RLlib in a day.

RLlib is already used in production by industry leaders in many different verticals,
such as manufacturing, finance, gaming, car makers, robotics, and others.

RLlib in 60 seconds
-------------------

It'll only take a few steps to get your first RLlib workload up and running on your laptop:

**TensorFlow or PyTorch**:

RLlib does not automatically install a deep-learning framework, but supports
TensorFlow (both 1.x with static-graph and 2.x with eager mode) as well as
PyTorch. Depending on your needs, make sure to install either TensorFlow or
PyTorch (or both as shown below):

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]" tensorflow torch

To be able to run our Atari examples, you should also install:

.. code-block:: bash

    $ pip install "gym[atari]" "gym[accept-rom-license]" atari_py

After these quick pip installs, you can start coding against RLlib.

Here is an example of running a PPO Trainer on the "`Taxi domain <https://gym.openai.com/envs/Taxi-v3/>`_"
for a few training iterations, then perform a single evaluation loop
(with rendering enabled):

.. literalinclude:: ../../../rllib/examples/documentation/rllib_in_60s.py
    :language: python
    :start-after: __rllib-in-60s-begin__
    :end-before: __rllib-in-60s-end__

`See here for a simple example on how to write an action inference loop after training. <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training.py>`_

Feature Overview
----------------

The following is a summary of RLlib's most striking features.
For an in-depth overview, check out our `documentation <http://docs.ray.io/en/latest/rllib.html>`_.

Click on the images below to see an example script for each of the listed features:


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1i_yoxocyEOgiCxcfRZVKpNh0R_-2tQZOX4syquiytAI/edit?skip_itp2_check=true&pli=1

        .. image:: ../images/rllib/sigils/rllib-sigil-tf-and-torch.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/custom_tf_policy.py

    .. container::

        The most **popular deep-learning frameworks**: `PyTorch <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_torch_policy.py>`_ and `TensorFlow
        (tf1.x/2.x static-graph/eager/traced) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_tf_policy.py>`_.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1yEOfeHvuLi5EzZKtGFQMfQ2NINzi3bUBrU3Z7bCiuKs/edit

        .. image:: ../images/rllib/sigils/rllib-sigil-distributed-learning.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/tune/framework.py

    .. container::

        **Highly distributed learning**: Our RLlib algorithms (such as our "PPO" or "IMPALA")
        allow you to set the ``num_workers`` config parameter, such that your workloads can run
        on 100s of CPUs/nodes thus parallelizing and speeding up learning.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1b8uaRo0KjPH-x-elBmyvDwAA4I2oy8cj3dxNnUT3HTE/edit

        .. image:: ../images/rllib/sigils/rllib-sigil-vector-envs.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/env_rendering_and_recording.py

    .. container::

        **Vectorized (batched) and remote (parallel) environments**: RLlib auto-vectorizes
        your ``gym.Envs`` via the ``num_envs_per_worker`` config. Environment workers can
        then batch and thus significantly speedup the action computing forward pass.
        On top of that, RLlib offers the ``remote_worker_envs`` config to create
        `single environments (within a vectorized one) as ray Actors <https://github.com/ray-project/ray/blob/master/rllib/examples/remote_vector_env_with_custom_api.py>`_,
        thus parallelizing even the env stepping process.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1Lbi1Zf5SvczSliGEWuK4mjWeehPIArYY9XKys81EtHU/edit

        .. image:: ../images/rllib/sigils/rllib-sigil-multi-agent.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_independent_learning.py

    .. container::

        | **Multi-agent RL** (MARL): Convert your (custom) ``gym.Envs`` into a multi-agent ones
          via a few simple steps and start training your agents in any of the following fashions:
        | 1) Cooperative with `shared <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`_ or
          `separate <https://github.com/ray-project/ray/blob/master/rllib/examples/two_step_game.py>`_
          policies and/or value functions.
        | 2) Adversarial scenarios using `self-play <https://github.com/ray-project/ray/blob/master/rllib/examples/self_play_with_open_spiel.py>`_
          and `league-based training <https://github.com/ray-project/ray/blob/master/rllib/examples/self_play_league_based_with_open_spiel.py>`_.
        | 3) `Independent learning <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_independent_learning.py>`_
          of neutral/co-existing agents.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1DY2IJUPo007mSRylz6IEs-dz_n1-rFh67RMi9PB2niY/edit

        .. image:: ../images/rllib/sigils/rllib-sigil-external-simulators.svg
            :width: 100
            :target: https://github.com/ray-project/ray/tree/master/rllib/examples/serving

    .. container::

        **External simulators**: Don't have your simulation running as a gym.Env in python?
        No problem! RLlib supports an external environment API and comes with a pluggable,
        off-the-shelve
        `client <https://github.com/ray-project/ray/blob/master/rllib/examples/serving/cartpole_client.py>`_/
        `server <https://github.com/ray-project/ray/blob/master/rllib/examples/serving/cartpole_server.py>`_
        setup that allows you to run 100s of independent simulators on the "outside"
        (e.g. a Windows cloud) connecting to a central RLlib Policy-Server that learns
        and serves actions. Alternatively, actions can be computed on the client side
        to save on network traffic.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1VFuESSI5u9AK9zqe9zKSJIGX8taadijP7Qw1OLv2hSQ/edit

        .. image:: ../images/rllib/sigils/rllib-sigil-offline-rl.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl.py

    .. container::

        **Offline RL and imitation learning/behavior cloning**: You don't have a simulator
        for your particular problem, but tons of historic data recorded by a legacy (maybe
        non-RL/ML) system? This branch of reinforcement learning is for you!
        RLlib's comes with several `offline RL <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl.py>`_
        algorithms (*CQL*, *MARWIL*, and *DQfD*), allowing you to either purely
        `behavior-clone <https://github.com/ray-project/ray/blob/master/rllib/agents/marwil/tests/test_bc.py>`_
        your existing system or learn how to further improve over it.


Customizations
--------------

RLlib provides simple APIs to customize all aspects of your training- and experimental workflows.
For example, you may code your own `environments <../rllib-env.html#configuring-environments>`__
in python using openAI's gym or DeepMind's OpenSpiel, provide custom
`TensorFlow/Keras- <../rllib-models.html#tensorflow-models>`__ or ,
`Torch models <../rllib-models.html#torch-models>`_, write your own
`policy- and loss definitions <../rllib-concepts.html#policies>`__, or define
custom `exploratory behavior <../rllib-training.htmlexploration-api>`_.

Via mapping one or more agents in your environments to (one or more) policies, multi-agent
RL (MARL) becomes an easy-to-use low-level primitive for our users.

.. figure:: ../images/rllib/rllib-stack.svg
    :align: left
    :width: 650

    **RLlib's API stack:** Built on top of Ray, RLlib offers off-the-shelf, highly distributed
    algorithms, policies, loss functions, and default models (including the option to
    auto-wrap a neural network with an LSTM or an attention net). Furthermore, our library
    comes with a built-in Server/Client setup, allowing you to connect
    hundreds of external simulators (clients) via the network to an RLlib server process,
    which provides learning functionality and serves action queries. User customizations
    are realized via sub-classing the existing abstractions and - by overriding certain
    methods in those sub-classes - define custom behavior.
