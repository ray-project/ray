RLlib Development
=================

Features
--------

Feature development and upcoming priorities are tracked on the `RLlib project board <https://github.com/ray-project/ray/projects/6>`__ (note that this may not include all development efforts). For discussion of issues and new features, we use the `Ray dev list <https://groups.google.com/forum/#!forum/ray-dev>`__ and `GitHub issues page <https://github.com/ray-project/ray/issues>`__.

Benchmarks
----------

Currently we host a number of full training run results in the `rl-experiments repo <https://github.com/ray-project/rl-experiments>`__, and maintain a list of working hyperparameter configurations in `tuned_examples <https://github.com/ray-project/ray/tree/master/python/ray/rllib/tuned_examples>`__. Benchmark results are extremely valuable to the community, so if you happen to have results that may be of interest, consider making a pull request to either repo.

Contributing Algorithms
-----------------------

These are the guidelines for merging new algorithms into RLlib:

* Contributed algorithms (`rllib/contrib <https://github.com/ray-project/ray/tree/master/python/ray/rllib/contrib>`__):
    - must subclass Agent and implement the ``_train()`` method
    - must include a lightweight test (`example <https://github.com/ray-project/ray/blob/6bb110393008c9800177490688c6ed38b2da52a9/test/jenkins_tests/run_multi_node_tests.sh#L45>`__) to ensure the algorithm runs
    - should include tuned hyperparameter examples and documentation
    - should offer functionality not present in existing algorithms

* Fully integrated algorithms (`rllib/agents <https://github.com/ray-project/ray/tree/master/python/ray/rllib/agents>`__) have the following additional requirements:
    - must fully implement the Agent API
    - must offer substantial new functionality not possible to add to other algorithms
    - should support custom models and preprocessors
    - should use RLlib abstractions and support distributed execution

Both integrated and contributed algorithms ship with the ``ray`` PyPI package, and are tested as part of Ray's automated tests. The main difference between contributed and fully integrated algorithms is that the latter will be maintained by the Ray team to a much greater extent with respect to bugs and integration with RLlib features.

How to add an algorithm to ``contrib``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It takes just two changes to add an algorithm to `contrib <https://github.com/ray-project/ray/tree/master/python/ray/rllib/contrib>`__. A minimal example can be found `here <https://github.com/ray-project/ray/tree/master/python/ray/rllib/contrib/random_agent/random_agent.py>`__. First, subclass `Agent <https://github.com/ray-project/ray/tree/master/python/ray/rllib/agents/agent.py>`__ and implement the ``_init`` and ``_train`` methods:

.. literalinclude:: ../../python/ray/rllib/contrib/random_agent/random_agent.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Second, register the agent with a name in `contrib/registry.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/contrib/registry.py>`__.

.. code-block:: python

    def _import_random_agent():
        from ray.rllib.contrib.random_agent.random_agent import RandomAgent
        return RandomAgent

    def _import_random_agent_2():
        from ray.rllib.contrib.random_agent_2.random_agent_2 import RandomAgent2
        return RandomAgent

    CONTRIBUTED_ALGORITHMS = {
        "contrib/RandomAgent": _import_random_agent,
        "contrib/RandomAgent2": _import_random_agent_2,
        # ...
    }

After registration, you can run and visualize agent progress using ``rllib train``:

.. code-block:: bash

    rllib train --run=contrib/RandomAgent --env=CartPole-v0
    tensorboard --logdir=~/ray_results
