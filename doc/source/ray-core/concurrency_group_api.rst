Limiting Concurrency Per-Method with Concurrency Groups
=======================================================

Besides setting the max concurrency overall for an asyncio actor, Ray allows methods to be separated into *concurrency groups*, each with its own asyncio event loop. This allows you to limit the concurrency per-method, e.g., allow a health-check method to be given its own concurrency quota separate from request serving methods.

.. warning:: Concurrency groups are only supported for asyncio actors, not threaded actors.

.. _defining-concurrency-groups:

Defining Concurrency Groups
---------------------------

You can define concurrency groups for asyncio actors using the ``concurrency_groups`` decorator argument:

.. tabbed:: Python

    This defines two concurrency groups, "io" with max_concurrency=2 and
    "compute" with max_concurrency=4.  The methods ``f1`` and ``f2`` are
    placed in the "io" group, and the methods ``f3`` and ``f4`` are placed
    into the "compute" group. Note that there is always a default
    concurrency group, which has a default concurrency of 1000.

    .. code-block:: python

        @ray.remote(concurrency_groups={"io": 2, "compute": 4})
        class AsyncIOActor:
            def __init__(self):
                pass

            @ray.method(concurrency_group="io")
            async def f1(self):
                pass

            @ray.method(concurrency_group="io")
            async def f2(self):
                pass

            @ray.method(concurrency_group="compute")
            async def f3(self):
                pass

            @ray.method(concurrency_group="compute")
            async def f4(self):
                pass

            async def f5(self):
                pass

        a = AsyncIOActor.remote()
        a.f1.remote()  # executed in the "io" group.
        a.f2.remote()  # executed in the "io" group.
        a.f3.remote()  # executed in the "compute" group.
        a.f4.remote()  # executed in the "compute" group.
        a.f5.remote()  # executed in the default group.


.. _default-concurrency-group:

Default Concurrency Group
-------------------------

By default, methods are placed in a default concurrency group which has a concurrency limit of 1000.
The concurrency of the default group can be changed by setting the ``max_concurrency`` actor option.

.. tabbed:: Python

    The following AsyncIOActor has 2 concurrency groups: "io" and "default".
    The max concurrency of "io" is 2, and the max concurrency of "default" is 10.

    .. code-block:: python

        @ray.remote(concurrency_groups={"io": 2)
        class AsyncIOActor:
            async def f1(self):
                pass

        actor = AsyncIOActor.options(max_concurrency=10).remote()


.. _setting-the-concurrency-group-at-runtime:

Setting the Concurrency Group at Runtime
----------------------------------------

You can also dispatch actor methods into a specific concurrency group at runtime using the ``.options`` method:

.. tabbed:: Python

    The following snippet demonstrates setting the concurrency group of the
    ``f2`` method dynamically at runtime.

    .. code-block:: python

        # Executed in the "io" group (as defined in the actor class).
        a.f2.options().remote()

        # Executed in the "compute" group.
        a.f2.options(concurrency_group="compute").remote()
