.. meta::
   :description: Give an actor a unique name in its namespace so any job in the cluster can retrieve it, with get-or-create and actor lifetime options.

Named Actors
============

An actor can be given a unique name within their :ref:`namespace <namespaces-guide>`.
This allows you to retrieve the actor from any job in the Ray cluster.
This can be useful if you cannot directly
pass the actor handle to the task that needs it, or if you are trying to
access an actor launched by another driver.
Note that the actor will still be garbage-collected if no handles to it
exist. See :ref:`actor-lifetimes` for more details.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            import ray

            @ray.remote
            class Counter:
                pass

            # Create an actor with a name
            counter = Counter.options(name="some_name").remote()

            # Retrieve the actor later somewhere
            counter = ray.get_actor("some_name")

.. note::

     Named actors are scoped by namespace. If no namespace is assigned, they will
     be placed in an anonymous namespace by default.

.. tab-set::

    .. tab-item:: Python

        .. testcode::
            :skipif: True

            import ray

            @ray.remote
            class Actor:
                pass

            # driver_1.py
            # Job 1 creates an actor, "orange" in the "colors" namespace.
            ray.init(address="auto", namespace="colors")
            Actor.options(name="orange", lifetime="detached").remote()

            # driver_2.py
            # Job 2 is now connecting to a different namespace.
            ray.init(address="auto", namespace="fruit")
            # This fails because "orange" was defined in the "colors" namespace.
            ray.get_actor("orange")
            # You can also specify the namespace explicitly.
            ray.get_actor("orange", namespace="colors")

            # driver_3.py
            # Job 3 connects to the original "colors" namespace
            ray.init(address="auto", namespace="colors")
            # This returns the "orange" actor we created in the first job.
            ray.get_actor("orange")

Get-Or-Create a Named Actor
---------------------------

A common use case is to create an actor only if it doesn't exist.
Ray provides a ``get_if_exists`` option for actor creation that does this out of the box.
This method is available after you set a name for the actor via ``.options()``.

If the actor already exists, a handle to the actor will be returned
and the arguments will be ignored. Otherwise, a new actor will be
created with the specified arguments.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ../doc_code/get_or_create.py


.. _actor-lifetimes:

Actor Lifetimes
---------------

Separately, actor lifetimes can be decoupled from the job, allowing an actor to persist even after the driver process of the job exits. We call these actors *detached*.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            counter = Counter.options(name="CounterActor", lifetime="detached").remote()

        The ``CounterActor`` will be kept alive even after the driver running above script
        exits. Therefore it is possible to run the following script in a different
        driver:

        .. testcode::

            counter = ray.get_actor("CounterActor")

        Note that an actor can be named but not detached. If we only specified the
        name without specifying ``lifetime="detached"``, then the CounterActor can
        only be retrieved as long as the original driver is still running.


Unlike normal actors, detached actors are not automatically garbage-collected by Ray.
Detached actors must be manually destroyed once you are sure that they are no
longer needed. To do this, use ``ray.kill`` to :ref:`manually terminate <ray-kill-actors>` the actor.
After this call, the actor's name may be reused.
