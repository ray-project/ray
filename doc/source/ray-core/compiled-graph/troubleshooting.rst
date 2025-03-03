Troubleshooting
===============

This page contains common issues and solutions for Compiled Graph execution.

Multiple executions with NumPy arrays 
-------------------------------------
Ray zero-copy deserializes NumPy arrays when possible. If you execute compiled graph with a NumPy array output multiple times, 
you could possibly run into issues if a NumPy array output from a previous Compiled Graph execution isn't deleted before attempting to get the result 
of a following execution of the same Compiled Graph. This is because the NumPy array stays in the buffer of the Compiled Graph until you or Python delete it. 
It's recommended to explicitly delete the NumPy array as Python may not always garbage collect the NumPy array immediately as you may expect.

For example, the following code sample could result in a RayChannelTimeoutError because Compiled Graph execution could hang if the NumPy array isn't deleted:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __numpy_troubleshooting_start__
    :end-before: __numpy_troubleshooting_end__

In the preceding code snippet, even though you may expect `result` to be garbage collected 
on each iteration of the loop, there is no guarantee of that from Python. Therefore, you should explicitly delete or 
copy and delete the NumPy array before you try to get the result of subsequent Compiled Graph executions.


Explicitly teardown before reusing the same actors
--------------------------------------------------
When reusing the same actors, it's important to explicitly teardown the Compiled Graph before reusing them.
Relying on the previous compiled graph to be garbage collected when expected can lead to issues
as Python may not always delete a Compiled Graph immediately as you may expect. 
For example, code such as the following could result in a segfault under specific conditions:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __teardown_troubleshooting_start__
    :end-before: __teardown_troubleshooting_end__
