.. _namespaces-guide:

Using Namespaces
================

A namespace is a logical grouping of jobs and named actors. When an actor is
named, its name must be unique within the namespace.

In order to set your applications namespace, it should be specified when you
first connect to the cluster.

.. code-block:: python

   ray.init(namespace="hello")
   # or using ray client
   ray.init("ray://<head_node_host>:10001", namespace="world")

Named actors are only accessible within their namespaces.

.. code-block:: python

    # `ray start --head` has been run to launch a local cluster
    import ray

    @ray.remote
    class Actor:
      pass

    # Job 1 creates two actors, "orange" and "purple" in the "colors" namespace.
    with ray.init("ray://localhost:10001", namespace="colors"):
      Actor.options(name="orange", lifetime="detached")
      Actor.options(name="purple", lifetime="detached")

    # Job 2 is now connecting to a different namespace.
    with ray.init("ray://localhost:10001", namespace="fruits")
      # This fails because "orange" was defined in the "colors" namespace.
      ray.get_actor("orange")
      # This succceeds because the name "orange" is unused in this namespace.
      Actor.options(name="orange", lifetime="detached")
      Actor.options(name="watermelon", lifetime="detached")

    # Job 3 connects to the original "colors" namespace
    context = ray.init("ray://localhost:10001", namespace="colors")
    # This fails because "watermelon" was in the fruits namespace.
    ray.get_actor("watermelon")
    # This returns the "orange" actor we created in the first job, not the second.
    ray.get_actor("orange")
    context.disconnect()
    # We are manually managing the scope of the connection in this example.


Anonymous namespaces
--------------------

When a namespace is not specified, Ray will place your job in an anonymous
namespace. In an anonymous namespace, your job will have its own namespace and
will not have access to actors in other namespaces.

.. code-block:: python

    # `ray start --head` has been run to launch a local cluster

    import ray

    @ray.remote
    class Actor:
      pass

    # Job 1 connects to an anonymous namespace by default
    ctx = ray.init("ray://localhost:10001")
    Actor.options(name="my_actor", lifetime="detached")
    ctx.disconnect()

    # Job 2 connects to a _different_ anonymous namespace by default
    ctx = ray.init("ray://localhost:10001")
    # This succeeds because the second job is in its own namespace.
    Actor.options(name="my_actor", lifetime="detached")
    ctx.disconnect()

.. note::

     Anonymous namespaces are implemented as UUID's. This makes it possible for
     a future job to manually connect to an existing anonymous namespace, but
     it is not recommended.


Getting the current namespace
-----------------------------
You can access to the current namespace using :ref:`runtime_context APIs <runtime-context-apis>`.

.. code-block:: python

    import ray
    ray.init(address="auto", namespace="colors")
    # Will print the information about "colors" namespace
    print(ray.get_runtime_context().namespace)

