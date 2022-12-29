Utility Classes
===============

Actor Pool
~~~~~~~~~~

.. tabbed:: Python

    The ``ray.util`` module contains a utility class, ``ActorPool``.
    This class is similar to multiprocessing.Pool and lets you schedule Ray tasks over a fixed pool of actors.

    .. literalinclude:: ../doc_code/actor-pool.py

    See the :ref:`package reference <ray-actor-pool-ref>` for more information.

.. tabbed:: Java

    Actor pool hasn't been implemented in Java yet.

.. tabbed:: C++

    Actor pool hasn't been implemented in C++ yet.

Message passing using Ray Queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes just using one signal to synchronize is not enough. If you need to send data among many tasks or
actors, you can use :ref:`ray.util.queue.Queue <ray-queue-ref>`.

.. literalinclude:: ../doc_code/actor-queue.py

Ray's Queue API has a similar API to Python's ``asyncio.Queue`` and ``queue.Queue``.
