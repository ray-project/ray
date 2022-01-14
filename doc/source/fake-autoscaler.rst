.. _fake-multinode:

Testing Autoscaling Locally
===========================

Testing autoscaling behavior is important for autoscaler development and the debugging of applications that depend
on autoscaler behavior. You can run the autoscaler locally without needing to launch a real cluster with one of the
following methods:

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
   :dedent: 4
   :start-after: __example_begin__
   :end-before: __example_end__

Python documentation:

.. autoclass:: ray.cluster_utils.AutoscalingCluster
    :members:

Features and Limitations of ``fake_multinode``
----------------------------------------------

Most of the features of the autoscaler are supported in fake multi-node mode. For example, if you update the contents of the YAML file, the autoscaler will pick up the new configuration and apply changes, as it does in a real cluster. Node selection, launch, and termination are governed by the same bin-packing and idle timeout algorithms as in a real cluster.

However, there are a few limitations:

1. All node raylets run uncontainerized on the local machine, and hence they share the same IP address. See the :ref:`fake_multinode_docker <fake-multinode-docker>` section for an alternative local multi node setup.

2. Configurations for auth, setup, initialization, Ray start, file sync, and anything cloud-specific are not supported.

3. It's necessary to limit the number of nodes / node CPU / object store memory to avoid overloading your local machine.

.. _fake-multinode-docker:

Testing containerized multi nodes locally with Docker compose
=============================================================
To go one step further and locally test a multi node setup where each node uses its own container (and thus
has a separate filesystem, IP address, and Ray processes), you can use the ``fake_multinode_docker`` node provider.

The setup is very similar to the :ref:`fake_multinode <fake-multinode>` provider. However, you need to start a monitoring process
(``docker_monitor.py``) that takes care of running the ``docker compose`` command.

Prerequisites:

1. Make sure you have `docker <https://docs.docker.com/get-docker/>`_ installed.

2. Make sure you have the `docker compose V2 plugin <https://docs.docker.com/compose/cli-command/#installing-compose-v2>`_ installed.

Using ``RAY_FAKE_CLUSTER=1 ray up``
-----------------------------------
Instructions:

1. Navigate to the root directory of the Ray repo you have cloned locally.

2. Locate the `fake_multi_node/example_docker.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml>`__ example file and fill in the number of CPUs and GPUs the local machine has for the head node type config. The YAML follows the same format as cluster autoscaler configurations, but some fields are not supported.

3. Configure worker types and other autoscaling configs as desired in the YAML file.

4. Make sure the ``shared_volume_dir`` is empty on the host system

5. Start the monitoring process:

.. code-block:: shell

    $ python ./python/ray/autoscaler/_private/fake_multi_node/docker_monitor.py \
        ./python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml

6. Start the Ray cluster using ``ray up``:

.. code-block:: shell

    $ RAY_FAKE_CLUSTER=1 ray up -y ./python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml

7. Connect your application to the fake local cluster with ``ray.init("ray://localhost:10002")``.

8. Alternatively, get a shell on the head node:

.. code-block:: shell

    $ docker exec -it fake_docker_fffffffffffffffffffffffffffffffffffffffffffffffffff00000_1 bash

Using ``ray.autoscaler._private.fake_multi_node.test_utils.DockerCluster``
--------------------------------------------------------------------------
This utility is used to write tests that use multi node behavior. The ``DockerCluster`` class can
be used to setup a Docker-compose cluster in a temporary directory, start the monitoring process,
wait for the cluster to come up, connect to it, and update the configuration.

Please see the API documentation and example test cases on how to use this utility.

.. autoclass:: ray.autoscaler._private.fake_multi_node.test_utils.DockerCluster
    :members:


Features and Limitations of ``fake_multinode_docker``
-----------------------------------------------------

The fake multinode docker node provider provides fully fledged nodes in their own containers. However,
some limitations still remain:

1. Configurations for auth, setup, initialization, Ray start, file sync, and anything cloud-specific are not supported
   (but might be in the future).

2. It's necessary to limit the number of nodes / node CPU / object store memory to avoid overloading your local machine.

3. In docker-in-docker setups, a careful setup has to be followed to make the fake multinode docker provider work (see below).

Setting up in a Docker-in-Docker (dind) environment
---------------------------------------------------
When setting up in a Docker-in-Docker (dind) environment (e.g. the Ray OSS Buildkite environment), some
things have to be kept in mind. To make this clear, consider these concepts:

* The **host** is the not-containerized machine on which the code is executed (e.g. Buildkite runner)
* The **outer container** is the container running directly on the **host**. In the Ray OSS Buildkite environment,
  two containers are started - a *dind* network host and a container with the Ray source code and wheel in it.
* The **inner container** is a container started by the fake multinode docker node provider.

The control plane for the multinode docker node provider lives in the outer container. However, ``docker compose``
commands are executed from the connected docker-in-docker network. In the Ray OSS Buildkite environment, this is
the ``dind-daemon`` container running on the host docker. If you e.g. mounted ``/var/run/docker.sock`` from the
host instead, it would be the host docker daemon. We will refer to both as the **host daemon** from now on.

The outer container modifies files that have to be mounted in the inner containers (and modified from there
as well). This means that the host daemon also has to have access to these files.

Similarly, the inner containers expose ports - but because the containers are actually started by the host daemon,
the ports are also only accessible on the host (or the dind container).

For the Ray OSS Buildkite environment, we thus set some environment variables:

* ``RAY_TEMPDIR="/ray-mount"``. This environment variable defines where the temporary directory for the
  cluster files should be created. This directory has to be accessible by the host, the outer container,
  and the inner container. In the inner container, we can control the directory name.

* ``RAY_HOSTDIR="/ray"``. In the case where the shared directory has a different name on the host, we can
  rewrite the mount points dynamically. In this example, the outer container is started with ``-v /ray:/ray-mount``
  or similar, so the directory on the host is ``/ray`` and in the outer container ``/ray-mount`` (see ``RAY_TEMPDIR``).

* ``RAY_TESTHOST="dind-daemon"`` As the containers are started by the host daemon, we can't just connect to
  ``localhost``, as the ports are not exposed to the outer container. Thus, we can set the Ray host with this environment
  variable.

Lastly, docker-compose obviously requires a docker image. The default docker image is ``rayproject/ray:nightly``.
The docker image requires ``openssh-server`` to be installed and enabled. In Buildkite we build a new image from
``rayproject/ray:nightly-py37-cpu`` to avoid installing this on the fly for every node (which is the default way).
This base image is built in one of the previous build steps.

Thus, we set

* ``RAY_DOCKER_IMAGE="rayproject/ray:multinode-py37"``

* ``RAY_HAS_SSH=1``

to use this docker image and inform our multinode infrastructure that SSH is already installed.

Local development
-----------------

If you're doing local development on the fake multi node docker module, you can set

* ``FAKE_CLUSTER_DEV="auto"``

this will mount the ``ray/python/ray/autoscaler`` directory to the started nodes. Please note that
this is will probably not work in your docker-in-docker setup.
