Ray design patterns
===================

This document is a collection of common design patterns (and anti-patterns) for Ray programs.
It is meant as a handbook for both:

- New users trying to understand how to get started with Ray, and
- Advanced users trying to optimize their Ray applications

This document is not meant as an introduction to Ray.
For that and any further questions that arise from this document,
please refer to
`A Gentle Introduction to Ray <https://docs.ray.io/en/master/ray-overview/index.html>`__,
the `Ray GitHub <https://github.com/ray-project/ray>`__, and the `Ray Slack <http://ray-distributed.slack.com>`__.
Highly technical users may also want to refer to the
`Ray 1.0 Architecture whitepaper <https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview>`__.

The patterns below are organized into "Basic Patterns,"
which are commonly seen in Ray applications, and "Advanced Patterns,"
which are less common but may be invaluable for certain use cases.


.. toctree::
   :maxdepth: 1
   :caption: Basic design patterns

   tree-of-actors.rst
   tree-of-tasks.rst
   map-reduce.rst
   limit-tasks.rst



.. toctree::
   :maxdepth: 1
   :caption: Basic design antipatterns

   global-variables.rst
   fine-grained-tasks.rst
   unnecessary-ray-get.rst
   closure-capture.rst
   ray-get-loop.rst


.. toctree::
   :maxdepth: 1
   :caption: Advanced design patterns

   overlapping-computation-communication.rst
   fault-tolerance-actor-checkpointing.rst
   concurrent-operations-async-actor.rst


.. toctree::
   :maxdepth: 1
   :caption: Advanced design antipatterns

   redefine-task-actor-loop.rst
   submission-order.rst
   too-many-results.rst

Contributing
------------
These documentation pages were created from a
`community-maintaned document <https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit#>`__.

In the document, you can suggest edits to the existing patterns and antipatterns.
There is also a list of patterns and antipattern which we would like to cover in the future.

If you want to contribute, just edit the document. Once reviewed, we will make sure
to reflect the changes on this documentation.
