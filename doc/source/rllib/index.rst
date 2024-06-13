.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. |tensorflow| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16

.. |pytorch| image:: images/pytorch.png
    :class: inline-figure
    :width: 16

.. |new_stack| image:: /rllib/images/sigils/new-api-stack.svg
    :class: inline-figure
    :width: 64

.. |old_stack| image:: /rllib/images/sigils/old-api-stack.svg
    :class: inline-figure
    :width: 64

.. |single_agent| image:: /rllib/images/sigils/single-agent.svg
    :class: inline-figure
    :width: 64

.. |multi_agent| image:: /rllib/images/sigils/multi-agent.svg
    :class: inline-figure
    :width: 64

.. _rllib-index:

.. image:: images/rllib-logo.png
    :align: center

RLlib: Industry-Grade Reinforcement Learning
============================================

.. toctree::
    :hidden:

    rllib-training
    key-concepts
    rllib-env
    rllib-algorithms
    user-guides
    rllib-examples
    rllib-new-api-stack
    package_ref/index


**RLlib** is an open source library for reinforcement learning (RL),
offering support for
production-level, highly distributed, scalable RL workloads, while maintaining unified
and simple APIs for a large variety of industry applications.

Whether training policies in a **multi-agent** setup, purely from **offline** (historic) data,
or using **externally connected simulators**, RLlib offers simple solutions for each of
these autonomous decision making needs and allows you to be up and running your experiments within hours.

RLlib is already used in production by industry leaders in many different verticals,
such as
`gaming <https://www.anyscale.com/events/2021/06/22/using-reinforcement-learning-to-optimize-iap-offer-recommendations-in-mobile-games>`_,
`robotics <https://www.anyscale.com/events/2021/06/23/introducing-amazon-sagemaker-kubeflow-reinforcement-learning-pipelines-for>`_,
`finance <https://www.anyscale.com/events/2021/06/22/a-24x-speedup-for-reinforcement-learning-with-rllib-+-ray>`_,
`climate control <https://www.anyscale.com/events/2021/06/23/applying-ray-and-rllib-to-real-life-industrial-use-cases>`_,
`industrial control <https://www.anyscale.com/events/2021/06/23/applying-ray-and-rllib-to-real-life-industrial-use-cases>`_,
`manufacturing and logistics <https://www.anyscale.com/events/2022/03/29/alphadow-leveraging-rays-ecosystem-to-train-and-deploy-an-rl-industrial>`_,
`automobile <https://www.anyscale.com/events/2021/06/23/using-rllib-in-an-enterprise-scale-reinforcement-learning-solution>`_,
`boat design <https://www.youtube.com/watch?v=cLCK13ryTpw>`_,
and many others.

RLlib in 60 seconds
-------------------

.. figure:: images/rllib-index-header.svg

It only takes a few steps to get your first RLlib workload up and running on your laptop.

RLlib doesn't automatically install a deep-learning framework, but supports **PyTorch**
and **TensorFlow** (2.x).

Depending on your needs, make sure to install one of these (or both), as shown below:

.. raw:: html

    <div class="termynal" data-termynal>
        <span data-ty="input">pip install "ray[rllib]" torch tensorflow</span>
    </div>

.. note::

    For installation on computers running Apple Silicon (such as M1),
    `follow instructions here. <https://docs.ray.io/en/latest/ray-overview/installation.html#m1-mac-apple-silicon-support>`_
    To be able to run our Atari examples, you should also install
    `pip install "gymnasium[atari]" "gymnasium[accept-rom-license]" ale_py`.

This is all you need to start coding against RLlib.
Here is an example of running the PPO Algorithm on the
`Taxi domain <https://www.gymlibrary.dev/environments/toy_text/taxi/>`_.
You first create a `config` for the algorithm, which sets the right RL environment and
defines all needed training parameters.

Next, `build` the algorithm and `train` it for a total of `5` iterations.
One training iteration includes parallel sample collection by the distributed :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors,
loss calculation on the collected data, and a model update.

As a last step, the trained Algorithm is evaluated:

.. literalinclude:: doc_code/rllib_in_60s.py
    :language: python
    :start-after: __rllib-in-60s-begin__
    :end-before: __rllib-in-60s-end__

Note that you can use any `Farama-Foundation Gymnasium <https://github.com/Farama-Foundation/Gymnasium>`__ environment as `env`.

In `config.env_runners()` you can specify - amongst many other things - the number of parallel
:py:class:`~ray.rllib.env.env_runner.EnvRunner` workers to collect samples from the environment.

You can also tweak RLlib's default `rl_module` config, and set up a separate config for the
evaluation :py:class:`~ray.rllib.env.env_runner.EnvRunner` s through the `config.evaluation()` method.

`See here <rllib-training.html#using-the-python-api>`_, if you want to learn more about the RLlib training APIs.
Also, `see here <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training.py>`_ for a simple example on how to write an action inference loop after training.

If you want to get a quick preview of which **algorithms** and **environments** RLlib supports,
click the dropdowns below:

.. dropdown:: **RLlib Algorithms**
    :animate: fade-in-slide-down

    * On-policy
       -  |single_agent| |multi_agent| |new_stack| |old_stack| :ref:`PPO (Proximal Policy Optimization) <ppo>`

    * Off-policy
       -  |single_agent| |multi_agent| |new_stack| |old_stack| :ref:`SAC (Soft Actor Critic) <sac>`
       -  |single_agent| |multi_agent| |new_stack| |old_stack| :ref:`DQN/Rainbow (Deep Q Networks) <dqn>`

    * High-throughput on- and off policy
       -  |single_agent| |multi_agent| |old_stack| :ref:`IMPALA (Importance Weighted Actor-Learner Architecture) <impala>`
       -  |single_agent| |multi_agent| |old_stack| :ref:`APPO (Asynchronous Proximal Policy Optimization) <appo>`

    *  Model-based RL

       -  |single_agent| |new_stack| :ref:`DreamerV3 <dreamerv3>`

    *  Offline RL and Imitation Learning
       -  |single_agent| |new_stack| |old_stack| :ref:`BC (Behavior Cloning) <bc>`
       -  |single_agent| |new_stack| |old_stack| :ref:`CQL (Conservative Q-Learning) <cql>`
       -  |single_agent| |new_stack| |old_stack| :ref:`MARWIL (Advantage Re-Weighted Imitation Learning) <marwil>`


.. dropdown:: **RLlib Environments**
    :animate: fade-in-slide-down

    *  `RLlib Environments Overview <rllib-env.html>`__
    *  `Farama-Foundation gymnasium <rllib-env.html#gymnasium>`__
    *  `Vectorized <rllib-env.html#vectorized>`__
    *  `Multi-Agent and Hierarchical <rllib-env.html#multi-agent-and-hierarchical>`__
    *  `External Agents and Applications <rllib-env.html#external-agents-and-applications>`__

       -  `External Application Clients <rllib-env.html#external-application-clients>`__

    *  `Advanced Integrations <rllib-env.html#advanced-integrations>`__

Feature Overview
----------------

.. grid:: 1 2 3 3
    :gutter: 1
    :class-container: container pb-4

    .. grid-item-card::

        **RLlib Key Concepts**
        ^^^
        Learn more about the core concepts of RLlib, such as environments, algorithms and
        policies.
        +++
        .. button-ref:: rllib-core-concepts
            :color: primary
            :outline:
            :expand:

            Key Concepts

    .. grid-item-card::

        **RLlib Algorithms**
        ^^^
        See the many available RL algorithms of RLlib for model-free and model-based
        RL, on-policy and off-policy training, multi-agent RL, and more.
        +++
        .. button-ref:: rllib-algorithms-doc
            :color: primary
            :outline:
            :expand:

            Algorithms

    .. grid-item-card::

        **RLlib Environments**
        ^^^
        Get started with environments supported by RLlib, such as Farama foundation's Gymnasium, Petting Zoo,
        and many custom formats for vectorized and multi-agent environments.
        +++
        .. button-ref:: rllib-environments-doc
            :color: primary
            :outline:
            :expand:

            Environments


The following is a summary of RLlib's most striking features.
Click the images below to see an example script for each of the listed features:

.. include:: feature_overview.rst


Customizing RLlib
-----------------

RLlib provides simple APIs to customize all aspects of your training- and experimental workflows.
For example, you may code your own `environments <rllib-env.html#configuring-environments>`__
in python using Farama-Foundation's gymnasium or DeepMind's OpenSpiel, provide custom
`Torch models <rllib-models.html#torch-models>`_, write your own
`policy- and loss definitions <rllib-concepts.html#policies>`__, or define
custom `exploratory behavior <rllib-training.html#exploration-api>`_.

By mapping one or more agents in your environment to (one or more) policies, multi-agent
RL (MARL) becomes an easy-to-use low-level primitive for RLlib users.

.. figure:: images/rllib-stack.svg
    :align: left
    :width: 650

    **RLlib's API stack:** Built on top of Ray, RLlib offers off-the-shelf, highly distributed
    algorithms, policies, loss functions, and default models (including the option to
    auto-wrap a neural network with an LSTM or an attention net). Furthermore, the library
    comes with a built-in Server/Client setup, allowing you to connect
    hundreds of external simulators (clients) through the network to an RLlib server process,
    which provides learning capability and serves action queries. User customizations
    are realized by sub-classing the existing abstractions and - by overriding certain
    methods in those sub-classes - define custom behavior.
