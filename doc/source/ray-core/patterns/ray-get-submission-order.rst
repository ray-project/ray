Anti-pattern: Processing results in submission order using ray.get increases runtime
====================================================================================

**Summary:** Avoid processing independent results in submission order using :func:`ray.get() <ray.get>` since results may be ready in a different order than the submission order.

A batch of tasks is submitted, and the code needs to process their results individually once they're done.
If each task takes a different amount of time to finish and the code processes results in submission order, it may waste time waiting for all of the slower (straggler) tasks that were submitted earlier to finish while later faster tasks have already finished.

Instead, process the tasks in the order that they finish using :func:`ray.wait() <ray.wait>` to speed up total time to completion.

.. figure:: ../images/ray-get-submission-order.svg

    Processing results in submission order vs completion order


Code example
------------

.. literalinclude:: ../doc_code/anti_pattern_ray_get_submission_order.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

Other ``ray.get()`` related anti-patterns are:

- :doc:`unnecessary-ray-get`
- :doc:`ray-get-loop`
