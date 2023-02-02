.. _fault-tolerance:

Fault Tolerance
===============

Ray is a distributed system, and that means failures can happen. Generally, failures can
be classified into two classes: 1) application-level failures, and 2)
system-level failures.  The former can happen because of bugs in user-level
code, or if external systems fail. The latter can be triggered by node
failures, network failures, or just bugs in Ray. Here, we describe the
mechanisms that Ray provides to allow applications to recover from failures.

To handle application-level failures, Ray provides mechanisms to catch errors,
retry failed code, and handle misbehaving code. See the pages for :ref:`task
<fault-tolerance-tasks>` and :ref:`actor <fault-tolerance-actors>` fault
tolerance for more information on these mechanisms.

Ray also provides several mechanisms to automatically recover from internal system-level failures. In particular, Ray can automatically recover from some failures in the :ref:`distributed object store <fault-tolerance-objects>`.


How to Write Fault Tolerant Ray Applications
--------------------------------------------

There are several recommendations to make Ray applications fault tolerant:

First, avoid letting the object outlive its :ref:`owner <fault-tolerance-objects>` task or actor.
As long as there are still references to an object,
the owner of an object keeps running even after the corresponding task or actor finishes.
If the owner fails, Ray :ref:`cannot recover <fault-tolerance-ownership>` the object automatically for those who try to access the object.
One example of creating such outlived objects is returning ``ray.put()`` ObjectRefs:

.. literalinclude:: doc_code/fault_tolerance_tips.py
    :language: python
    :start-after: __return_ray_put_start__
    :end-before: __return_ray_put_end__

In the above example, object ``x`` outlives its owner task ``A``.
If the worker process running task ``A`` fails, getting ``x``'s value afterwards will receive an ``OwnerDiedError`` exception.
In contrast, object ``y`` is owned by the driver and it's only accessed within the lifetime of the driver.
If ``y`` is lost, Ray can automatically recover it via :ref:`lineage reconstruction <fault-tolerance-objects-reconstruction>`.
See :doc:`/ray-core/patterns/return-ray-put` for more details.

Returning a child task result ObjectRef can also create an object that outlives its owner:

.. literalinclude:: doc_code/fault_tolerance_tips.py
    :language: python
    :start-after: __return_child_task_start__
    :end-before: __return_child_task_end__

Second, avoid using :ref:`custom resource requirements <custom-resources>` that can only be satisfied by a particular node.
If the particular node fails, the running tasks or actors cannot be retried since there will be no other nodes that meet the resource requirements.

.. literalinclude:: doc_code/fault_tolerance_tips.py
    :language: python
    :start-after: __node_ip_resource_start__
    :end-before: __node_ip_resource_end__

Last but not least, if the fault tolerance mechanisms provided by Ray don't work for you,
you can always catch :ref:`exceptions <ray-core-exceptions>` caused by failures and recover manually.


More about Ray Fault Tolerance
------------------------------

.. toctree::
    :maxdepth: 1

    fault_tolerance/tasks.rst
    fault_tolerance/actors.rst
    fault_tolerance/objects.rst
