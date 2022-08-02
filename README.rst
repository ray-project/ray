.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://readthedocs.org/projects/ray/badge/?version=master
    :target: http://docs.ray.io/en/master/?badge=master

.. image:: https://img.shields.io/badge/Ray-Join%20Slack-blue
    :target: https://forms.gle/9TSdDYUgxYs8SA9e8

.. image:: https://img.shields.io/badge/Discuss-Ask%20Questions-blue
    :target: https://discuss.ray.io/

.. image:: https://img.shields.io/twitter/follow/raydistributed.svg?style=social&logo=twitter
    :target: https://twitter.com/raydistributed

|

Ray is a unified framework for scaling AI and Python applications. Ray consists of a core distributed runtime and a toolkit of libraries (Ray AIR) for accelerating ML workloads:

.. image:: https://github.com/ericl/ray/raw/ray-intro/doc/source/images/what-is-ray.svg

Learn more about `Ray AIR`_ and its libraries:
- `Datasets`_: Distributed Data Loading and Compute
- `Train`_: Distributed Deep Learning
- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `Serve`_: Scalable and Programmable Serving

Or more about `Ray Core`_ and its key abstractions:
- `Tasks`_: Stateless tasks executed in the cluster.
- `Actors`_: Stateful worker processes created in the cluster.
- `Objects`_: Shared-memory objects accessible across the cluster.

Install Ray with: ``pip install ray``. For nightly wheels, see the
`Installation page <https://docs.ray.io/en/master/installation.html>`__.

.. _`Serve`: https://docs.ray.io/en/master/serve/index.html
.. _`Datasets`: https://docs.ray.io/en/master/data/dataset.html
.. _`Workflow`: https://docs.ray.io/en/master/workflows/concepts.html
.. _`Train`: https://docs.ray.io/en/master/train/train.html
.. _`Tune`: https://docs.ray.io/en/master/tune/index.html
.. _`RLlib`: https://docs.ray.io/en/master/rllib/index.html

More Information
----------------

- `Ray Documentation`_
- `Ray Architecture whitepaper`_
- `Exoshuffle: large-scale data shuffle in Ray`_
- `RLlib paper`_
- `Tune paper`_

*Older documents:*

- `Ray paper`_
- `Ray HotOS paper`_

.. _`Ray AIR`: https://docs.ray.io/en/master/ray-air/getting-started.html
.. _`Ray Core`: https://docs.ray.io/en/master/ray-core/walkthrough.html
.. _`Tasks`: https://docs.ray.io/en/master/ray-core/tasks.html
.. _`Actors`: https://docs.ray.io/en/master/ray-core/actors.html
.. _`Objects`: https://docs.ray.io/en/master/ray-core/objects.html
.. _`Documentation`: http://docs.ray.io/en/master/index.html
.. _`Ray Architecture whitepaper`: https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview
.. _`Exoshuffle: large-scale data shuffle in Ray`: https://arxiv.org/abs/2203.05072
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

Getting Involved
----------------

.. list-table::
   :widths: 25 50 25 25
   :header-rows: 1

   * - Platform
     - Purpose
     - Estimated Response Time
     - Support Level
   * - `Discourse Forum`_
     - For discussions about development and questions about usage.
     - < 1 day
     - Community
   * - `GitHub Issues`_
     - For reporting bugs and filing feature requests.
     - < 2 days
     - Ray OSS Team
   * - `Slack`_
     - For collaborating with other Ray users.
     - < 2 days
     - Community
   * - `StackOverflow`_
     - For asking questions about how to use Ray.
     - 3-5 days
     - Community
   * - `Meetup Group`_
     - For learning about Ray projects and best practices.
     - Monthly
     - Ray DevRel
   * - `Twitter`_
     - For staying up-to-date on new features.
     - Daily
     - Ray DevRel

.. _`Discourse Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Meetup Group`: https://www.meetup.com/Bay-Area-Ray-Meetup/
.. _`Twitter`: https://twitter.com/raydistributed
.. _`Slack`: https://forms.gle/9TSdDYUgxYs8SA9e8

