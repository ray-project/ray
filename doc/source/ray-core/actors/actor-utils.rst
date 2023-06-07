Utility Classes
===============

Actor Pool
~~~~~~~~~~

.. tab-set::

    .. tab-item:: Python

        The ``ray.util`` module contains a utility class, ``ActorPool``.
        This class is similar to multiprocessing.Pool and lets you schedule Ray tasks over a fixed pool of actors.

        .. literalinclude:: ../doc_code/actor-pool.py

        See the :class:`package reference <ray.util.ActorPool>` for more information.

    .. tab-item:: Java

        Actor pool hasn't been implemented in Java yet.

    .. tab-item:: C++

        Actor pool hasn't been implemented in C++ yet.

Message passing using Ray Queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes just using one signal to synchronize is not enough. If you need to send data among many tasks or
actors, you can use :class:`ray.util.queue.Queue <ray.util.queue.Queue>`.

.. literalinclude:: ../doc_code/actor-queue.py

Ray's Queue API has a similar API to Python's ``asyncio.Queue`` and ``queue.Queue``.
