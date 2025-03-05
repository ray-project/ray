Troubleshooting
===============

This page contains common issues and solutions for Compiled Graph execution.

Returning NumPy arrays
-------------------------------------
Ray zero-copy deserializes NumPy arrays when possible. If you execute compiled graph with a NumPy array output multiple times, 
you could possibly run into issues if a NumPy array output from a previous Compiled Graph execution isn't deleted before attempting to get the result 
of a following execution of the same Compiled Graph. This is because the NumPy array stays in the buffer of the Compiled Graph until you or Python delete it. 
It's recommended to explicitly delete the NumPy array as Python may not always garbage collect the NumPy array immediately as you may expect.

For example, the following code sample could result in a hang or RayChannelTimeoutError if the NumPy array isn't deleted:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __numpy_troubleshooting_start__
    :end-before: __numpy_troubleshooting_end__

In the preceding code snippet, Python may not garbage collect the NumPy array in `result` on each iteration of the loop. 
Therefore, you should explicitly delete the NumPy array before you try to get the result of subsequent Compiled Graph executions.


Explicitly teardown before reusing the same actors
--------------------------------------------------
If you want to reuse actors after using them in a Compiled Graph, it's important to explicitly teardown the Compiled Graph before reusing the actors. 
If you don't explicitly teardown the Compiled Graph, the actors may not behave as expected in future use because Ray opens channels on 
actors for Compiled Graph execution.

For example, the following code could result in a segfault because Python could delay garbage collection (which triggers implicit teardown of the first Compiled Graph) until the point the second Compiled Graph is being created:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __teardown_troubleshooting_start__
    :end-before: __teardown_troubleshooting_end__
