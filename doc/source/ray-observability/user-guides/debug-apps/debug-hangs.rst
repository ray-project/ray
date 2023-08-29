.. _observability-debug-hangs:

Debugging Hangs
===============
View stack traces in Ray Dashboard
-----------------------------------
The :ref:`Ray dashboard <observability-getting-started>`  lets you profile Ray Driver or Worker processes, by clicking on the "CPU profiling" or "Stack Trace" actions for active Worker processes, Tasks, Actors, and Job's driver process.

.. image:: /images/profile.png
   :align: center
   :width: 80%

Clicking "Stack Trace" will return the current stack trace sample using ``py-spy``. By default, only the Python stack
trace is shown. To show native code frames, set the URL parameter ``native=1`` (only supported on Linux).

.. image:: /images/stack.png
   :align: center
   :width: 60%

.. note::
   If you run Ray in a Docker container, you may run into permission errors when viewing the stack traces. Follow the `py-spy documentation`_ to resolve it. If you are a KubeRay user, here is the guide for how to :ref:`configure KubeRay and apply the solution <kuberay-pyspy-integration>`.
   
.. _`py-spy documentation`: https://github.com/benfred/py-spy#how-do-i-run-py-spy-in-docker


Use ``ray stack`` CLI command
------------------------------

Once ``py-spy`` is installed, you can run ``ray stack`` to dump the stack traces of all Ray Worker processes on
the current node.

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
