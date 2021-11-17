RLlib: Industry-Grade Reinforcement Learning with TF and Torch
==============================================================

**RLlib** is an open-source library for reinforcement learning (RL), offering support for
production-level, highly distributed RL workloads, while maintaining
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


Installation and Setup
----------------------

Install RLlib and run your first experiment on your laptop in seconds:

**TensorFlow:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]" tensorflow "gym[atari]" "gym[accept-rom-license]" atari_py
    $ rllib train --run APPO --env CartPole-v0


**PyTorch:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]" torch "gym[atari]" "gym[accept-rom-license]" atari_py
    $ rllib train --run APPO --env CartPole-v0 --torch


Quick First Experiment
----------------------

.. literalinclude:: examples/documentation/rllib_on_ray_readme.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

For a more detailed `"60 second" example, head to our main documentation  <https://docs.ray.io/en/latest/rllib/index.html>`_.


Highlighted Features
--------------------

The following is a summary of RLlib's most striking features (for an in-depth overview,
check out our `documentation <http://docs.ray.io/en/master/rllib/index.html>`_):

.. include:: ../doc/source/rllib/feature_overview.rst

In-Depth Documentation
----------------------

For an in-depth overview of RLlib and everything it has to offer, including
hand-on tutorials of important industry use cases and workflows, head over to
our `documentation pages <https://docs.ray.io/en/master/rllib/index.html>`_.


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
