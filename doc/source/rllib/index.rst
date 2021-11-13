.. admonition:: We're hiring!

    The RLlib team at `Anyscale Inc. <https://anyscale.com>`__, the company behind Ray, is hiring interns and full-time **reinforcement learning engineers** to help advance and maintain RLlib.
    If you have a background in ML/RL and are interested in making RLlib **the** industry-leading open-source RL library, `apply here today <https://jobs.lever.co/anyscale/186d9b8d-3fee-4e07-bb8e-49e85cf33d6b>`__.
    We'd be thrilled to welcome you on the team!

.. _rllib-index:

RLlib: Industry-Grade Reinforcement Learning with PyTorch and TensorFlow
========================================================================

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

RLlib is already used in production by industry leaders from many different verticals,
such as manufacturing, logistics, finance, gaming, automakers, robotics, and lots of others.

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

To be able to run our Atari examples we well, you should also install:

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

Feature Overview
----------------

The following is a summary of RLlib's most striking features (for an in-depth overview, check out our `documentation <http://docs.ray.io/en/master/rllib.html>`_).

In particular, RLlib offers and supports:

.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/tf-and-torch.svg
..            :width: 120

.. container::

    The most **popular deep-learning frameworks**: PyTorch and TensorFlow
    (tf1.x/2.x static-graph/eager/traced).


.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/distributed-learning.svg
..            :width: 120

.. container::

    **Highly distributed learning**: Our RLlib algorithms (such as our "PPO" or "IMPALA")
    allow you to set the ``num_workers`` config parameter, such that your workloads can run
    on 100s of CPUs/nodes thus parallelizing and speeding up learning.

.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/vectorized-envs.svg
..            :width: 120

.. container::

    **Vectorized (batched) environments**: RLlib auto-vectorizes your (custom)
    gym.Env classes such that RLlib environment workers can batch and thus
    significantly speedup the action computing model forward passes.

.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/multi-agent.svg
..            :width: 120

.. container::

    **Multi-agent RL** (MARL): Convert your (custom) gym.Env into a multi-agent one via a few simple steps and start training your agents in any of the following possible setups:
    <br/>Cooperative with shared or separate policies and/or value functions.
    <br/>Adversarial scenarios using self-play and league-based training.
    <br/>Independent learning of neutral/co-existing agents.

.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/external-simulators.svg
..            :width: 120

.. container::

    **External simulators** connecting to RLlib from the outside (e.g.
    via http(s)): Don't have your simulation running as a gym.Env in python?
    No problem, RLlib supports an external environment API and comes with a pluggable,
    off-the-shelve client/server setup that allows you to run 100s of independent
    simulators on the "outside" (e.g. a Windows cloud) connecting to a central RLlib
    Policy-Server that learns and serves actions. Alternatively, actions can be computed
    on the client side to save on network traffic.

.. .. container:: clear-both

..    .. container:: buttons-float-left

..        .. image:: ../images/rllib/sigils/offline-rl.svg
..            :width: 120

.. container::

    **Offline (batch) RL and imitation learning (behavior cloning)** using historic data:
    If you don't have a simulator for your particular problem, but tons of historic data
    recorded by a legacy (maybe non-RL/ML system), this branch of reinforcement learning
    is for you! RLlib's comes with several offline RL algorithms (*CQL*, *MARWIL*, and *DQfD*),
    allowing you to either purely behavior-clone the historic system (the one that recorded
    your historic data) or learn how to improve over that system.


In-Depth Documentation
----------------------

For an in-depth overview of RLlib and everything it has to offer, including
hand-on tutorials of important industry use cases and workflows, head over to
our `documentation pages <https://docs.ray.io/en/master/rllib.html>`_.


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

    **RLlib's API stack:** Built on top of Ray, RLlib offers off-the-shelf, highly distributed
    algorithms, policies, loss functions, and default models (including the option to
    auto-wrap a neural network with an LSTM or an attention net). Furthermore, our library
    comes with a built-in Server/Client setup, allowing you to connect
    hundreds of external simulators (clients) via the network to an RLlib server process,
    which provides learning functionality and serves action queries. User customizations
    are realized via sub-classing the existing abstractions and - by overriding certain
    methods in those sub-classes - define custom behavior.


To learn more, proceed to the `table of contents <rllib-toc.html>`__.
