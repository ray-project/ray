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
   ray.client().namespace("world").connect()

Named actors are only accessible within their namespaces.

.. code-block:: python

    import

    @ray.remote
    class Actor:
      pass

    # Job 1 creates two actors, "orange" and "purple" in the "colors" namespace.
    ray.client().namespace("colors").connect()
    Actor.options(name="orange", lifetime="detached")
    Actor.options(name="purple", lifetime="detached")
    ray.util.disconnect()

    # Job 2 is now connecting to a different namespace.
    ray.client().namespace("fruits").connect()
    # This fails because "orange" was defined in the "colors" namespace.
    ray.get_actor("orange")
    # This succceeds because the name "orange" is unused in this namespace.
    Actor.options(name="orange", lifetime="detached")
    Actor.options(name="watermelon", lifetime="detached")
    ray.util.disconnect()

    # Job 3 connects to the original "colors" namespace
    ray.client().namespace("colors").connect()
    # This fails because "watermelon" was in the fruits namespace.
    ray.get_actor("watermelon")
    # This returns the "orange" actor we created in the first job, not the second.
    ray.get_actor("orange")
    ray.util.disconnect()
         

Anonymous namespaces
--------------------

When a namespace is not specified, Ray will place your job in an anonymous
namespace. In an anonymous namespace, your job will have its own namespace and
will not have access to actors in other namespaces.

.. code-block:: python

    import

    @ray.remote
    class Actor:
      pass

    # Job 1 connects to an anonymous namespace by default
    ray.client().connect()
    Actor.options(name="my_actor", lifetime="detached")
    ray.util.disconnect()

    # Job 2 connects to an _different_ anonymous namespace by default
    ray.client().connect()
    # This succeeds because the second job is in its own namespace.
    Actor.options(name="my_actor", lifetime="detached")
    ray.util.disconnect()

.. note::

     Anonymous namespaces are implemented as UUID's. This makes it possible for
     a future job to manually connect to an existing anonymous namespace, but
     it is not recommended.

