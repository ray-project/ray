Testing Autoscaling Locally
===========================

Testing autoscaling behavior is important for autoscaler development and the debugging of applications that depend on autoscaler behavior. You can run the autoscaler locally without needing to launch a real cluster with one of the following methods:

Using ``RAY_FAKE_CLUSTER=1 ray start``
--------------------------------------

Instructions:

1. Navigate to the root directory of the Ray repo you have cloned locally.

2. Locate the `fake_multi_node/example.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/fake_multi_node/example.yaml>`__ example file and fill in the number of CPUs and GPUs the local machine has for the head node type config. The YAML follows the same format as cluster autoscaler configurations, but some fields are not supported.

3. Configure worker types and other autoscaling configs as desired in the YAML file.

4. Start the fake cluster locally:

.. code-block:: shell

    $ ray stop --force
    $ RAY_FAKE_CLUSTER=1 ray start \
        --autoscaling-config=./python/ray/autoscaler/_private/fake_multi_node/example.yaml \
        --head --block

5. Connect your application to the fake local cluster with ``ray.init("auto")``.

6. Run ``ray status`` to view the status of your cluster, or ``cat /tmp/ray/session_latest/logs/monitor.*`` to view the autoscaler monitor log:

.. code-block:: shell

   $ ray status
   ======== Autoscaler status: 2021-10-12 13:10:21.035674 ========
   Node status
   ---------------------------------------------------------------
   Healthy:
    1 ray.head.default
    2 ray.worker.cpu
   Pending:
    (no pending nodes)
   Recent failures:
    (no failures)

   Resources
   ---------------------------------------------------------------
   Usage:
    0.0/10.0 CPU
    0.00/70.437 GiB memory
    0.00/10.306 GiB object_store_memory

   Demands:
    (no resource demands)

Using ``ray.cluster_utils.AutoscalingCluster``
----------------------------------------------

To programmatically create a fake multi-node autoscaling cluster and connect to it, you can use `cluster_utils.AutoscalingCluster <https://github.com/ray-project/ray/blob/master/python/ray/cluster_utils.py>`__. Here's an example of a basic autoscaling test that launches tasks triggering autoscaling:

.. literalinclude:: /../../python/ray/tests/test_autoscaler_fake_multinode.py
   :language: python
   :start-after: __example_begin__
   :end-before: __example_end__

Python documentation:

.. autoclass:: ray.cluster_utils.AutoscalingCluster
    :members:

Features and Limitations
------------------------

Most of the features of the autoscaler are supported in fake multi-node mode. For example, if you update the contents of the YAML file, the autoscaler will pick up the new configuration and apply changes, as it does in a real cluster. Node selection, launch, and termination are governed by the same bin-packing and idle timeout algorithms as in a real cluster.

However, there are a few limitations:

1. All node raylets run uncontainerized on the local machine, and hence they share the same IP address.

2. Configurations for auth, setup, initialization, Ray start, file sync, and anything cloud-specific are not supported.

3. It's necessary to limit the number of nodes / node CPU / object store memory to avoid overloading your local machine.
