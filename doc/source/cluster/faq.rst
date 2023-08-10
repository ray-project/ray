.. _cluster-FAQ:

===
FAQ
===

These are some Frequently Asked Questions that we've seen pop up for using Ray clusters.
If you still have questions after reading this FAQ,  please reach out on
`our Discourse <https://discuss.ray.io/>`__!

Do Ray clusters support multi-tenancy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, you can run multiple :ref:`jobs <jobs-overview>` from different users simultaneously in a Ray cluster
but it's NOT recommended in production.
The reason is that Ray currently still misses some features for multi-tenancy in production:

* Ray doesn't provide strong resource isolation:
  Ray :ref:`resources <core-resources>` are logical and they don't limit the physical resources a task or actor can use while running.
  This means simultaneous jobs can interfere with each other and makes them less reliable to run in production.

* Ray doesn't support priorities: All jobs, tasks and actors have the same priority so there is no way to prioritize important jobs under load.

* Ray doesn't support access control: jobs have full access to a Ray cluster and all of the resources within it.

On the other hand, you can run the same job multiple times using the same cluster to save the cluster startup time.

.. note::
    A Ray :ref:`namespace <namespaces-guide>` is just a logical grouping of jobs and named actors. Unlike a Kubernetes namespace, it doesn't provide any other multi-tenancy functions like resource quotas.


I have multiple Ray users. What's the right way to deploy Ray for them?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's recommended to start a Ray cluster for each user so that their workloads are isolated.

What is the difference between ``--node-ip-address`` and ``--address``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When starting a head node on a machine with more than one network address, you
may need to specify the externally-available address so worker nodes can
connect. This is done with:

.. code:: bash

    ray start --head --node-ip-address xx.xx.xx.xx --port nnnn``

Then when starting the worker node, use this command to connect to the head node:

.. code:: bash

    ray start --address xx.xx.xx.xx:nnnn

What does a worker node failure to connect look like?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the worker node cannot connect to the head node, you should see this error

    Unable to connect to GCS at xx.xx.xx.xx:nnnn. Check that (1) Ray GCS with
    matching version started successfully at the specified address, and (2)
    there is no firewall setting preventing access.

The most likely cause is that the worker node cannot access the IP address
given. You can use ``ip route get xx.xx.xx.xx`` on the worker node to start
debugging routing issues.

You may also see failures in the log like

    This node has an IP address of xx.xx.xx.xx, while we can not found the
    matched Raylet address. This maybe come from when you connect the Ray
    cluster with a different IP address or connect a container.

which can be caused by overloading the head node with too many simultaneous
connections. The solution for this is to start the worker nodes more slowly.

I am having problems getting my SLURM cluster to work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There seem to be a class of issues starting ray on SLURM clusters.  While we
have not been able to pin down the exact causes (as of June 2023), work has
been done to mitigate some of the resource contention. Some of the issues
reported:

* Using a machine with a large number of CPUs, and starting one worker per CPU
  together with OpenBLAS (as used in NumPy) may allocate too many threads. This
  is an `known OpenBLAS limitation`_ and can be mitigated by limiting OpenBLAS
  to one thread per process as explained in the link.

* Resource allocation is not what was expected: usually too many CPUs per node
  were allocated. Best practice is to verify your SLURM configuration without
  starting ray to verify that the allocations are as expected. For more
  detailed information see :ref:`ray-slurm-deploy`.

.. _`known OpenBLAS limitation`: https://github.com/xianyi/OpenBLAS/wiki/faq#how-can-i-use-openblas-in-multi-threaded-applications  
