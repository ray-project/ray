.. _observability-debug-hangs:

Debugging Hangs
===============
View stack traces in Ray Dashboard
------------------
The :ref:`Ray dashboard <observability-getting-started>`  lets you profile Ray worker processes by clicking on the "Stack Trace"
actions for active worker processes, actors, and job's driver process.

.. image:: /images/profile.png
   :align: center
   :width: 80%

Clicking "Stack Trace" will return the current stack trace sample using ``py-spy``. By default, only the Python stack
trace is shown. To show native code frames, set the URL parameter ``native=1`` (only supported on Linux).

.. image:: /images/stack.png
   :align: center
   :width: 60%


Use ``ray stack`` CLI command
------------------

You can run ``ray stack`` to dump the stack traces of all Ray worker processes on
the current node. This requires ``py-spy`` to be installed. See the `Troubleshooting page <troubleshooting.html>`_ for more details.

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
