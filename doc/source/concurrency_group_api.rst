AsyncIO with Concurrency Group
================================
Besides setting max concurrency for an asyncio actor, Ray offers Concurrency Group concept for you to define different eventloop to execute different asyncio methods.
Detailly, Every concurrency group hold a unqiue eventloop to execute the async methods which belong this group. And the methods with no declaration with concurrency group will executed in a default concurreny group.


.. _define-concurrency-groups:

Define Concurrency Groups for AsyncIO Actors
------------------
You can define the concurrency groups for your asyncio actors with adding `concurrency_groups` key word arguement for the actor annotation `@ray.remote`.

.. tabs::
  .. group-tab:: Python

    Define 2 concurrency groups, "io" with 2 concurrency limitation and "compute" with 4 concurrency limitation,
    and declare `f1` and `f2` with "io" group, and declare `f3` `f4` with "compute" group as follows.

    So there are 3 concurrency groups: "io", "compute" and "default".
    Note that max concurrency of "default" group is 1000 by default.
    Tasks of `f1` and `f2` will be executed in "io" group and tasks of `f3` and `f4` will be executed in "compute" group.
    Tasks of `f5` will be executed in "default" group.
    
    .. code-block:: python

        @ray.remote(concurrency_groups={"io": 2, "compute": 4})
        class AsyncIOActor:
            def __init__(self):
                pass

            @ray.method(concurrency_group="io")
            async def f1(self):
                pass

            @ray.method(concurrency_group="io")
            def f2(self):
            pass

            @ray.method(concurrency_group="compute")
            def f3(self):
                pass

            @ray.method(concurrency_group="compute")
            def f4(self):
                pass

            def f5(self):
                pass

        a = AsyncIOActor.remote()
        a.f1.remote()  # executed in the "io" group.
        a.f2.remote()  # executed in the "io" group.
        a.f3.remote()  # executed in the "compute" group.
        a.f4.remote()  # executed in the "compute" group.
        a.f5.remote()  # executed in the default group.



.. _specify-concurrency-group-for-methods-dynamically:

Declare Using Concurrency Group for Task at Runtime
------------------

Besides declaring using concurrency groups for methods at method definitions of actors. We can also declare it when invoke remote tasks at runtime.

.. tabs::
  .. group-tab:: Python

    The following snippet shows that 1st task of `f2` declaring using "compute" group. So the 1st task of `f2` will be executed in "compute" group and the 2nd task of `f2` will still be executed in "io" group.

    .. code-block:: python
        a.f2.options(concurrency_group=”compute”).remote() # executed in "compute" group.
        a.f2.options().remote() # executed in "compute" group.



.. _interact-with-set-max-concurrency

Interact With max_concurrency Flag
------------------

The flag `max_concurrency` indicates the max concurrency number of the default concurrency group.

.. tabs::
  .. group-tab:: Python

    The following snippet shows the AsyncIOActor has 2 concurrency groups: "io" and "default".
    The max concurrency of "io" group is 2, and the max concurrency of "default" group is 10.

    .. code-block:: python
        @ray.remote(concurrency_groups={"io": 2)
        class AsyncIOActor:
            async def f1(self):
                pass

        actor = AsyncIOActor.options(max_concurrency=10).remote()


If you don't specify the max_concurrency for "default" group, it's 1000 by default.

.. tabs::
  .. group-tab:: Python

    The following snippet shows the AsyncIOActor has 2 concurrency groups: "io" and "default".
    The max concurrency of "io" group is 2, and the max concurrency of "default" group is 1000.

    .. code-block:: python
        @ray.remote(concurrency_groups={"io": 2)
        class AsyncIOActor:
            async def f1(self):
                pass

        actor = AsyncIOActor.remote()



.. _more-attentions

Attentions
------------------
There are 2 things you should keep in mind when using AsyncIO actors with Concurrency Group.

1. The "default" is a reserved name in Ray Concurrency Group, so you should not name your concurrency group name "default". Otherwise you will get a `ValueError`.

2. Concurrency Group is only supported in AsyncIO actor, not in Threaded Actor. You should not use it in your Threaded Actor.

