RLlib: Industry-Grade Reinforcement Learning with TF and Torch
==============================================================

**RLlib** is an open-source library for reinforcement learning, offering support for
production-level, highly distributed reinforcement learning (RL) workloads while maintaining
unified and simple APIs for a large variety of industry applications.
Whether you would like to train your agents in multi-agent setups,
purely from offline (historic) datasets, or using externally
connected simulators, RLlib offers simple solutions for your decision making needs.

You **don't need** to be an **RL expert** to use RLlib, nor do you need to learn Ray or any
other of its libraries! If you either have your problem coded (in python) as an
"`RL environment <https://github.com/openai/gym>`_" or own lots of pre-recorded, historic
behavioral data to learn from, you will be up and running in only a few days.

RLlib is already used in production by industry leaders in many different verticals,
such as manufacturing, logistics, finance, gaming, automakers, robotics,
and many others.


Quick Setup
-----------

You'll only need 30 seconds (and the following commands) to get your first RLlib workload
up and running on your laptop:

**TensorFlow:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]"
    $ pip install tensorflow
    $ rllib train --run APPO --env CartPole-v0


**PyTorch:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]"
    $ pip install torch
    $ rllib train --run APPO --env CartPole-v0 --torch


Feature Overview
----------------

The following is a summary of RLlib's most striking features (for an in-depth overview, check out our `documentation <http://docs.ray.io/en/master/rllib.html>`_).

In particular, RLlib offers and supports:

1) The most **popular deep-learning frameworks**:
   PyTorch and TensorFlow (tf1.x/2.x static-graph/eager/traced).

1) **Highly distributed learning**: Typical RLlib algorithms (such as our "PPO"
   or "IMPALA") allow you to set the ``num_workers`` config parameter,
   such that your workloads can run on 100s of CPUs/nodes thus parallelizing and
   speeding up learning.

1) **Vectorized (batched) environments**: RLlib auto-vectorizes your (custom)
   gym.Env classes such that RLlib environment workers can batch and thus
   significantly speedup the action computing model forward passes.

2) **Multi-agent RL** (MARL): Convert your (custom) gym.Env into a multi-agent one
   via a few simple steps and start training your agents in any possible setup:

   a) Cooperative with shared or separate policies and/or value functions.

   b) Adversarial scenarios using self-play and league-based training.

   c) Independent learning of neutral/co-existing agents.

3) **External simulators** connecting to RLlib from the outside (e.g. via http(s)):
   Don't have your simulation running as a gym.Env in python? No problem, RLlib supports
   an external environment API and comes with a pluggable, off-the-shelve client/server
   setup that allows you to run 100s of independent simulators on the "outside"
   (e.g. a Windows cloud) connecting to a central RLlib PolicyServer that learns and
   may serve actions (actions can also be computed on the client side to save on network
   traffic).

4) **Arbitrarily nested observation- and action spaces**:

5) **Offline (batch) RL and imitation learning (behavior cloning)** using historic data:
   If you don't have a simulator for your particular problem, but tons of historic data
   recorded by a legacy (maybe non-RL/ML system), this branch of reinforcement learning
   is for you! RLlib's comes with several offline RL algorithms
   (*CQL*, *MARWIL*, and *DQfD*), allowing you to either purely behavior-clone
   the historic system (the one that recorded your historic data) or learn how to improve
   over that system.

In-Depth Documentation
----------------------

For an in-depth overview of RLlib and everything it has to offer, including
hand-on tutorials of important industry use cases and workflows, head over to
our `documentation pages <https://docs.ray.io/en/latest/rllib.html>`_.


Cite our Paper
--------------

If you've found RLlib useful for your research, please cite our `paper <https://arxiv.org/abs/1712.09381>`_ as follows:

.. code-block::

    @inproceedings{liang2018rllib,
        Author = {Eric Liang and
                  Richard Liaw and
                  Robert Nishihara and
                  Philipp Moritz and
                  Roy Fox and
                  Ken Goldberg and
                  Joseph E. Gonzalez and
                  Michael I. Jordan and
                  Ion Stoica},
        Title = {{RLlib}: Abstractions for Distributed Reinforcement Learning},
        Booktitle = {International Conference on Machine Learning ({ICML})},
        Year = {2018}
    }
