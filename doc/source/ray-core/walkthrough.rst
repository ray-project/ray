.. _core-walkthrough:

What's Ray Core?
=================

.. toctree::
    :maxdepth: 1
    :hidden:

    Key Concepts <key-concepts>
    User Guides <user-guide>
    Examples <examples/overview>
    api/index


Ray Core is a powerful distributed computing framework that provides a small set of essential primitives (tasks, actors, and objects) for building and scaling distributed applications.
This walk-through introduces you to these core concepts with simple examples that demonstrate how to transform your Python functions and classes into distributed Ray tasks and actors, and how to work effectively with Ray objects.

.. note::

    Ray has introduced an experimental API for high-performance workloads that's
    especially well suited for applications using multiple GPUs.
    See :ref:`Ray Compiled Graph <ray-compiled-graph>` for more details.


Getting Started
---------------

To get started, install Ray using ``pip install -U ray``. For additional installation options, see :ref:`Installing Ray <installation>`.

The first step is to import and initialize Ray:

.. literalinclude:: doc_code/getting_started.py
    :language: python
    :start-after: __starting_ray_start__
    :end-before: __starting_ray_end__

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` is automatically called on the first use of a Ray remote API.

Running a Task
--------------

Tasks are the simplest way to parallelize your Python functions across a Ray cluster. To create a task:

1. Decorate your function with ``@ray.remote`` to indicate it should run remotely
2. Call the function with ``.remote()`` instead of a normal function call
3. Use ``ray.get()`` to retrieve the result from the returned future (Ray *object reference*)

Here's a simple example:

.. literalinclude:: doc_code/getting_started.py
    :language: python
    :start-after: __running_task_start__
    :end-before: __running_task_end__

Calling an Actor
----------------

While tasks are stateless, Ray actors allow you to create stateful workers that maintain their internal state between method calls. 
When you instantiate a Ray actor:

1. Ray starts a dedicated worker process somewhere in your cluster
2. The actor's methods run on that specific worker and can access and modify its state
3. The actor executes method calls serially in the order it receives them, preserving consistency

Here's a simple Counter example:

.. literalinclude:: doc_code/getting_started.py
    :language: python
    :start-after: __calling_actor_start__
    :end-before: __calling_actor_end__

The preceding example demonstrates basic actor usage. For a more comprehensive example that combines both tasks and actors, see the :ref:`Monte Carlo Pi estimation example <monte-carlo-pi>`.

Passing Objects
---------------

Ray's distributed object store efficiently manages data across your cluster. There are three main ways to work with objects in Ray:

1. **Implicit creation**: When tasks and actors return values, they are automatically stored in Ray's :ref:`distributed object store <objects-in-ray>`, returning *object references* that can be later retrieved.
2. **Explicit creation**: Use ``ray.put()`` to directly place objects in the store.
3. **Passing references**: You can pass object references to other tasks and actors, avoiding unnecessary data copying and enabling lazy execution.


Here's an example showing these techniques:

.. literalinclude:: doc_code/getting_started.py
    :language: python
    :start-after: __passing_object_start__
    :end-before: __passing_object_end__

Next Steps
----------

.. tip:: To monitor your application's performance and resource usage, check out the :ref:`Ray dashboard <observability-getting-started>`.

You can combine Ray's simple primitives in powerful ways to express virtually any distributed computation pattern. To dive deeper into Ray's :ref:`key concepts <core-key-concepts>`,
explore these user guides:

.. grid:: 1 2 3 3
    :gutter: 1
    :class-container: container pb-3


    .. grid-item-card::
        :img-top: /images/tasks.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: ray-remote-functions

            Using remote functions (Tasks)

    .. grid-item-card::
        :img-top: /images/actors.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: ray-remote-classes

            Using remote classes (Actors)

    .. grid-item-card::
        :img-top: /images/objects.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: objects-in-ray

            Working with Ray Objects
