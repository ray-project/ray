Anti-pattern: Redefining the same remote function or class harms performance
============================================================================

**Summary:** Avoid redefining the same remote function or class.

Decorating the same function or class multiple times using the :func:`ray.remote <ray.remote>` decorator leads to slow performance in Ray.
For each Ray remote function or class, Ray pickles it and uploads to GCS.
Later on, the worker that runs the task or actor downloads and unpickles it.
Each decoration of the same function or class generates a new remote function or class from Ray's perspective.
As a result, the pickle, upload, download, and unpickle work happens every time the code redefines and runs the remote function or class.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_redefine_task_actor_loop.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_redefine_task_actor_loop.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__

Define the same remote function or class outside of the loop instead of multiple times inside a loop so that it's pickled and uploaded only once.
