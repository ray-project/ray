.. meta::
   :description: Group jobs and named actors into namespaces so actor names need only be unique per namespace, including anonymous namespaces.

.. _namespaces-guide:

Using Namespaces
================

A namespace is a logical grouping of jobs and named actors. When an actor is
named, its name must be unique within the namespace.

In order to set your applications namespace, it should be specified when you
first connect to the cluster.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/namespaces.py
          :language: python
          :start-after: __init_namespace_start__
          :end-before: __init_namespace_end__

Named actors are only accessible within their namespaces.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/namespaces.py
          :language: python
          :start-after: __actor_namespace_start__
          :end-before: __actor_namespace_end__

Specifying namespace for named actors
-------------------------------------

You can specify a namespace for a named actor while creating it. The created actor belongs to
the specified namespace, no matter what namespace of the current job is.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/namespaces.py
          :language: python
          :start-after: __specify_actor_namespace_start__
          :end-before: __specify_actor_namespace_end__


Anonymous namespaces
--------------------

When a namespace is not specified, Ray will place your job in an anonymous
namespace. In an anonymous namespace, your job will have its own namespace and
will not have access to actors in other namespaces.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/namespaces.py
          :language: python
          :start-after: __anonymous_namespace_start__
          :end-before: __anonymous_namespace_end__

.. note::

     Anonymous namespaces are implemented as UUID's. This makes it possible for
     a future job to manually connect to an existing anonymous namespace, but
     it is not recommended.


Getting the current namespace
-----------------------------
You can access to the current namespace using :ref:`runtime_context APIs <runtime-context-apis>`.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/namespaces.py
          :language: python
          :start-after: __get_namespace_start__
          :end-before: __get_namespace_end__
