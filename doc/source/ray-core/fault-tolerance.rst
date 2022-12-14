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

Ray also provides several mechanisms to automatically recover from internal system-level failures. In particular, Ray can automatically recover from some failures in the :ref:`distributed object store <fault-tolerance-objects>` and in the :ref:`global control service <fault-tolerance-gcs>`, which handles cluster-wide metadata.


.. toctree::
    :maxdepth: 1

    fault_tolerance/tasks.rst
    fault_tolerance/actors.rst
    fault_tolerance/objects.rst
    fault_tolerance/ray_components.rst
