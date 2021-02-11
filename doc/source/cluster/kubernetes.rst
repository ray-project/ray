***********************
Deploying on Kubernetes
***********************

.. _ray-k8s-deploy:

Introduction
============
You can leverage your Kubernetes cluster as a substrate for execution of distributed Ray programs.
The Ray Autoscaler spins up and deletes Kubernetes pods according to resource demands of the Ray workload - each Ray node runs in its own Kubernetes pod.

Quick Guide
-----------

This document covers the following topics:

- :ref:`Overview of methods for launching a Ray Cluster on Kubernetes<k8s-overview>`
- :ref:`Managing clusters with the Ray Cluster Launcher<k8s-cluster-launcher>`
- :ref:`Managing clusters with the Ray Kubernetes Operator<k8s-operator>`
- :ref:`Interacting with a Ray Cluster via a Kubernetes Service<ray-k8s-interact>`
- :ref:`Comparison of the Ray Cluster Launcher and Ray Kubernetes Operator<k8s-comparison>`

You can find more information at the following links:

- :ref:`GPU usage with Kubernetes<k8s-gpus>`
- :ref:`Using Ray Tune on your Kubernetes cluster<tune-kubernetes>`
- :ref:`How to manually set up a non-autoscaling Ray cluster on Kubernetes<ray-k8s-static>`

.. _k8s-overview:

Ray on Kubernetes
=================

Ray supports two ways of launching an autoscaling Ray cluster on Kubernetes.

- Using the :ref:`Ray Cluster Launcher <k8s-cluster-launcher>`
- Using the :ref:`Ray Kubernetes Operator <k8s-operator>`

The Cluster Launcher and Ray Kubernetes Operator provide similar functionality; each serves as an `interface to the Ray autoscaler`.
Below is a brief overview of the two tools.

The Ray Cluster Launcher
------------------------
The :ref:`Ray Cluster Launcher <ref-automatic-cluster>` is geared towards experimentation and development and can be used to launch Ray clusters on Kubernetes (among other backends).
It allows you to manage an autoscaling Ray Cluster from your local environment using the :ref:`Ray CLI <cluster-commands>`.
For example, you can use ``ray up`` to launch a Ray cluster on Kubernetes and ``ray exec`` to execute commands in the Ray head node's pod.
Note that using the Cluster Launcher requires Ray to be :ref:`installed locally <installation>`.

* Get started with the :ref:`Ray Cluster Launcher on Kubernetes<k8s-cluster-launcher>`.

The Ray Kubernetes Operator
---------------------------
The Ray Kubernetes Operator is a Kubernetes-native solution geared towards production use cases.
Rather than handling cluster launching locally, cluster launching and autoscaling are centralized in the Operator's Pod.
The Operator follows the standard Kubernetes `pattern <https://kubernetes.io/docs/concepts/extend-kubernetes/operator/>`__ - it runs
a control loop which manages a `Kubernetes Custom Resource`_ specifying the desired state of your Ray cluster.
Using the Kubernetes Operator does not require a local installation of Ray - all interactions with your Ray cluster are mediated by Kubernetes.

* Get started with the :ref:`Ray Kubernetes Operator<k8s-operator>`.


Further reading
---------------

Read :ref:`here<k8s-comparison>` for more details on the comparison between the Operator and Cluster Launcher.
Note that it is also possible to manually deploy a :ref:`non-autoscaling Ray cluster <ray-k8s-static>` on Kubernetes.

.. note::

  The configuration ``yaml`` files used in this document are provided in the `Ray repository`_
  as examples to get you started. When deploying real applications, you will probably
  want to build and use your own container images, add more worker nodes to the
  cluster, and change the resource requests for the head and worker nodes. Refer to the provided ``yaml``
  files to be sure that you maintain important configuration options for Ray to
  function properly.


.. _`Ray repository`: https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes

.. _k8s-cluster-launcher:

Managing Clusters with the Ray Cluster Launcher
===============================================

This section briefly explains how to use the Ray Cluster Launcher to launch a Ray cluster on your existing Kubernetes cluster.

First, install the Kubernetes API client (``pip install kubernetes``), then make sure your Kubernetes credentials are set up properly to access the cluster (if a command like ``kubectl get pods`` succeeds, you should be good to go).

Once you have ``kubectl`` configured locally to access the remote cluster, you should be ready to launch your cluster. The provided `ray/python/ray/autoscaler/kubernetes/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__ cluster config file will create a small cluster of one pod for the head node configured to autoscale up to two worker node pods, with all pods requiring 1 CPU and 0.5GiB of memory.

Test that it works by running the following commands from your local machine:

.. _cluster-launcher-commands:

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to get a remote shell into the head node.
    $ ray up ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # List the pods running in the cluster. You shoud only see one head node
    # until you start running an application, at which point worker nodes
    # should be started. Don't forget to include the Ray namespace in your
    # 'kubectl' commands ('ray' by default).
    $ kubectl -n ray get pods

    # Get a remote screen on the head node.
    $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml
    $ # Try running a Ray program with 'ray.init(address="auto")'.

    # View monitor logs
    $ ray monitor ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # Tear down the cluster
    $ ray down ray/python/ray/autoscaler/kubernetes/example-full.yaml

* Learn about :ref:`running Ray programs on Kubernetes <ray-k8s-run>`

.. _k8s-operator:

Managing clusters with the Ray Kubernetes Operator
==================================================

.. role:: bash(code)
   :language: bash

This section explains how to use the Ray Kubernetes Operator to launch a Ray cluster on your existing Kubernetes cluster.

The example commands in this document launch six Kubernetes pods, using a total of 6 CPU and 3.5Gi memory.
If you are experimenting using a test Kubernetes environment such as `minikube`_, make sure to provision sufficient resources, e.g.
:bash:`minikube start --cpus=6 --memory=\"4G\"`.
Alternatively, reduce resource usage by editing the ``yaml`` files referenced in this document; for example, reduce ``minWorkers``
in ``example_cluster.yaml`` and ``example_cluster2.yaml``.

.. note::

   1. The Ray Kubernetes Operator is still experimental. For the yaml files in the examples below, we recommend using the latest master version of Ray.
   2. The Ray Kubernetes Operator requires Kubernetes version at least ``v1.17.0``. Check Kubernetes version info with the command :bash:`kubectl version`.


Applying the RayCluster Custom Resource Definition
--------------------------------------------------
The Ray Kubernetes operator works by managing a user-submitted `Kubernetes Custom Resource`_ (CR) called a ``RayCluster``.
A RayCluster custom resource describes the desired state of the Ray cluster.

To get started, we need to apply the `Kubernetes Custom Resource Definition`_ (CRD) defining a RayCluster.


.. code-block:: shell

 $ kubectl apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml

 customresourcedefinition.apiextensions.k8s.io/rayclusters.cluster.ray.io created

.. note::

    The file ``cluster_crd.yaml`` defining the CRD is not meant to meant to be modified by the user. Rather, users :ref:`configure <operator-launch>` a RayCluster CR via a file like `ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml>`__.
    The Kubernetes API server then validates the user-submitted RayCluster resource against the CRD.

Picking a Kubernetes Namespace
-------------------------------
The rest of the Kubernetes resources we will use are `namespaced`_.
You can use an existing namespace for your Ray clusters or create a new one if you have permissions.
For this example, we will create a namespace called ``ray``.

.. code-block:: shell

 $ kubectl create namespace ray

 namespace/ray created

Starting the Operator
----------------------

To launch the operator in our namespace, we execute the following command.

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml

 serviceaccount/ray-operator-serviceaccount created
 role.rbac.authorization.k8s.io/ray-operator-role created
 rolebinding.rbac.authorization.k8s.io/ray-operator-rolebinding created
 pod/ray-operator-pod created

The output shows that we've launched a Pod named ``ray-operator-pod``. This is the pod that runs the operator process.
The ServiceAccount, Role, and RoleBinding we have created grant the operator pod the `permissions`_ it needs to manage Ray clusters.

.. _operator-launch:

Launching Ray Clusters
----------------------
Finally, to launch a Ray cluster, we create a RayCluster custom resource.

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml

 raycluster.cluster.ray.io/example-cluster created

The operator detects the RayCluster resource we've created and launches an autoscaling Ray cluster.
Our RayCluster configuration specifies ``minWorkers:2`` in the second entry of ``spec.podTypes``, so we get a head node and two workers upon launch.

.. note::

  For more details about RayCluster resources, we recommend take a looking at the annotated example `example_cluster.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml>`__  applied in the last command.

.. code-block:: shell

 $ kubectl -n ray get pods
 NAME                               READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-hbxvv     1/1     Running   0          72s
 example-cluster-ray-worker-4hvv6   1/1     Running   0          64s
 example-cluster-ray-worker-78kp5   1/1     Running   0          64s
 ray-operator-pod                   1/1     Running   0          2m33s

We see four pods: the operator, the Ray head node, and two Ray worker nodes.

Let's launch another cluster in the same namespace, this one specifiying ``minWorkers:1``.

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

We confirm that both clusters are running in our namespace.

.. code-block:: shell

 $ kubectl -n ray get rayclusters
 NAME               STATUS    AGE
 example-cluster    Running   19s
 example-cluster2   Running   19s


 $ kubectl -n ray get pods
 NAME                                READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-th4wv      1/1     Running   0          10m
 example-cluster-ray-worker-q9pjn    1/1     Running   0          10m
 example-cluster-ray-worker-qltnp    1/1     Running   0          10m
 example-cluster2-ray-head-kj5mg     1/1     Running   0          10s
 example-cluster2-ray-worker-qsgnd   1/1     Running   0          1s
 ray-operator-pod                    1/1     Running   0          10m

Now we can :ref:`run Ray programs<ray-k8s-run>` on our Ray clusters.

.. _operator-logs:

Monitoring
----------
Autoscaling logs are written to the operator pod's ``stdout`` and can be accessed with :code:`kubectl logs`.
Each line of output is prefixed by the name of the cluster followed by a colon.
The following command gets the last hundred lines of autoscaling logs for our second cluster.

.. code-block:: shell

 $ kubectl -n ray logs ray-operator-pod | grep ^example-cluster2: | tail -n 100

The output should include monitoring updates that look like this:

.. code-block:: shell

    example-cluster2:2020-12-12 13:55:36,814        DEBUG autoscaler.py:693 -- Cluster status: 1 nodes
    example-cluster2: - MostDelayedHeartbeats: {'172.17.0.4': 0.04093289375305176, '172.17.0.5': 0.04084634780883789}
    example-cluster2: - NodeIdleSeconds: Min=36 Mean=38 Max=41
    example-cluster2: - ResourceUsage: 0.0/2.0 CPU, 0.0/1.0 Custom1, 0.0/1.0 is_spot, 0.0 GiB/0.58 GiB memory, 0.0 GiB/0.1 GiB object_store_memory
    example-cluster2: - TimeSinceLastHeartbeat: Min=0 Mean=0 Max=0
    example-cluster2:Worker node types:
    example-cluster2: - worker-nodes: 1
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:148 -- Cluster resources: [{'object_store_memory': 1.0, 'node:172.17.0.4': 1.0, 'memory': 5.0, 'CPU': 1.0}, {'object_store_memory': 1.0, 'is_spot': 1.0, 'memory': 6.0, 'node:172.17.0.5': 1.0, 'Custom1': 1.0, 'CPU': 1.0}]
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:149 -- Node counts: defaultdict(<class 'int'>, {'head-node': 1, 'worker-nodes
    ': 1})
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:159 -- Placement group demands: []
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:186 -- Resource demands: []
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:187 -- Unfulfilled demands: []
    example-cluster2:2020-12-12 13:55:36,891        INFO resource_demand_scheduler.py:209 -- Node requests: {}
    example-cluster2:2020-12-12 13:55:36,903        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-tdxdr is not being updated and passes config check (can_update=True).
    example-cluster2:2020-12-12 13:55:36,923        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-tdxdr is not being updated and passes config check (can_update=True).

Cleaning Up
-----------
We shut down a Ray cluster by deleting the associated RayCluster resource.
Either of the next two commands will delete our second cluster ``example-cluster2``.

.. code-block:: shell

 $ kubectl -n ray delete raycluster example-cluster2
 # OR
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

The pods associated with ``example-cluster2``  go into the ``TERMINATING`` phase. In a few moments, we check that these pods are gone:

.. code-block:: shell

 $ kubectl -n ray get pods
 NAME                               READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-th4wv     1/1     Running   0          57m
 example-cluster-ray-worker-q9pjn   1/1     Running   0          56m
 example-cluster-ray-worker-qltnp   1/1     Running   0          56m
 ray-operator-pod                   1/1     Running   0          57m

Only the operator pod and the first ``example-cluster`` remain.

To finish clean-up, we delete the cluster ``example-cluster`` and then the operator's resources.

.. code-block:: shell

 $ kubectl -n ray delete raycluster example-cluster
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml

If you like, you can delete the RayCluster customer resource definition.
(Using the operator again will then require reapplying the CRD.)

.. code-block:: shell

 $ kubectl delete crd rayclusters.cluster.ray.io
 # OR
 $ kubectl delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml


.. _ray-k8s-interact:

Interacting with a Ray Cluster
==============================
:ref:`Ray Client <ray-client>` allows you to connect to your Ray cluster on Kubernetes and execute Ray programs.
The Ray Client server runs the Ray head node, by default on port 10001.

:ref:`Ray Dashboard <ray-dashboard>` gives visibility into the state of your cluster.
By default, the dashboard uses port 8265 on the Ray head node.

.. _k8s-service:

Configuring a head node service
-------------------------------
To use Ray Client and Ray Dashboard,
you can connect via a `Kubernetes Service`_ targeting the relevant ports on the head node:

.. _svc-example:

.. code-block:: yaml

    apiVersion: v1
    kind: Service
    metadata:
        name: example-cluster-ray-head
    spec:
        # This selector must match the head node pod's selector.
        selector:
            component: example-cluster-ray-head
        ports:
            - name: client
              protocol: TCP
              port: 10001
              targetPort: 10001
            - name: dashboard
              protocol: TCP
              port: 8265
              targetPort: 8265


The head node pod's ``metadata`` should have a ``label`` matching the service's ``selector`` field:

.. code-block:: yaml

    apiVersion: v1
    kind: Pod
    metadata:
      # Automatically generates a name for the pod with this prefix.
      generateName: example-cluster-ray-head-
      # Must match the head node service selector above if a head node
      # service is required.
      labels:
          component: example-cluster-ray-head

- The Ray Kubernetes Operator automatically configures a default service exposing ports 10001 and 8265 \
  on the head node pod. The Operator also adds the relevant label to the head node pod's configuration. \
  If this default service does not suit your use case, you can modify the service or create a new one, \
  for example by using the tools ``kubectl edit``, ``kubectl create``, or ``kubectl apply``.

- The Ray Cluster launcher does not automatically configure a service targeting the head node. A \
  head node service can be specified in the cluster launching config's ``provider.services`` field. The example cluster lauching \
  config `example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__ includes \
  the :ref:`above <svc-example>` service configuration as an example.

After launching a Ray cluster with either the Operator or Cluster Launcher, you can view the configured service:

.. code-block:: shell

 $ kubectl -n ray get services

  NAME                       TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)              AGE
  example-cluster-ray-head   ClusterIP   10.106.123.159   <none>        10001/TCP,8265/TCP   52s

.. _ray-k8s-run:

Running Ray Programs
--------------------
Given a running Ray cluster and a :ref:`Service <k8s-service>` exposing the Ray Client server's port on the head pod,
we can now run Ray programs on our cluster.

In the following examples, we assume that we have a running Ray cluster with one head node and
two worker nodes. This can be achieved in one of two ways:

- Using the :ref:`Operator <k8s-operator>` with the example resource `ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml>`__.
- Using :ref:`Cluster Launcher <k8s-cluster-launcher>`. Modify the example file `ray/python/ray/autoscaler/kubernetes/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__
  by setting the field ``available_node_types.worker_node.min_workers``
  to 2 and then run ``ray up`` with the modified config.


Using Ray Client to connect from within the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can connect to your Ray cluster from another pod in the same Kubernetes cluster.

For example, you can submit a Ray application to run on the Kubernetes cluster as a `Kubernetes
Job`_. The Job will run a single pod running the Ray driver program to
completion, then terminate the pod but allow you to access the logs.

The following command submits a Job which executes an `example Ray program`_.

.. code-block:: yaml

  $ kubectl create -f ray/python/ray/autoscaler/kubernetes/job-example.yaml

The program executed by the Job waits for three Ray nodes to connect and then tests object transfer
between the nodes. Note that the program uses the environment variables
``EXAMPLE_CLUSTER_RAY_HEAD_SERVICE_HOST`` and ``EXAMPLE_CLUSTER_RAY_HEAD_SERVICE_PORT_CLIENT``
to access Ray Client. These `environment variables`_ are set by Kubernetes based on
the service we are using to expose the Ray head node.

To view the output of the Job, first find the name of the pod that ran it,
then fetch its logs:

.. code-block:: shell

  $ kubectl -n ray get pods
  NAME                               READY   STATUS    RESTARTS   AGE
  example-cluster-ray-head-rpqfb     1/1     Running   0          11m
  example-cluster-ray-worker-4c7cn   1/1     Running   0          11m
  example-cluster-ray-worker-zvglb   1/1     Running   0          11m
  ray-test-job-8x2pm-77lb5           1/1     Running   0          8s

  # Fetch the logs. You should see repeated output for 10 iterations and then
  # 'Success!'
  $ kubectl -n ray logs ray-test-job-8x2pm-77lb5

To clean up the resources created by the Job after checking its output, run
the following:

.. code-block:: shell

  # List Jobs run in the Ray namespace.
  $ kubectl -n ray get jobs
  NAME                 COMPLETIONS   DURATION   AGE
  ray-test-job-kw5gn   1/1           10s        30s

  # Delete the finished Job.
  $ kubectl -n ray delete job ray-test-job-kw5gn

  # Verify that the Job's pod was cleaned up.
  $ kubectl -n ray get pods
  NAME                               READY   STATUS    RESTARTS   AGE
  example-cluster-ray-head-rpqfb     1/1     Running   0          11m
  example-cluster-ray-worker-4c7cn   1/1     Running   0          11m
  example-cluster-ray-worker-zvglb   1/1     Running   0          11m

.. _`environment variables`: https://kubernetes.io/docs/concepts/services-networking/service/#environment-variables
.. _`example Ray program`: https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/example_scripts/job_example.py


Using Ray Client to connect from outside the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To connect to the Ray cluster from outside your Kubernetes cluster,
the head node Service needs to communicate with the outside world.

One way to achieve this is by port-forwarding.
Run the following command locally:

.. code-block:: shell

  $ kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001

`Alternatively`, you can find the head node pod and connect to it directly with
the following command:

.. code-block:: shell

  # Substitute the name of your Ray cluster if using a name other than "example-cluster".
  $ kubectl -n ray port-forward \
    $(kubectl -n ray get pods -l ray-cluster-name=example-cluster -l  ray-node-type=head -o custom-columns=:metadata.name) 10001:10001

Then open a new shell and try out a sample program:

.. code-block:: shell

  $ python ray/python/ray/autoscaler/kubernetes/example_scripts/run_local_example.py

The program in this example uses ``ray.util.connect(127.0.0.1:10001)`` to connect to the Ray cluster.

.. note::

  Connecting with Ray client requires using the matching minor versions of Python (for example 3.7)
  on the server and client end -- that is on the Ray head node and in the environment where
  ``ray.util.connect`` is invoked. Note that the default ``rayproject/ray`` images use Python 3.7.
  Nightly builds are now available for Python 3.6 and 3.8 at the `Ray Docker Hub <https://hub.docker.com/r/rayproject/ray/tags?page=1&ordering=last_updated&name=nightly-py>`_.

Running the program on the head node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It is also possible to execute a Ray program on the Ray head node.
(Replace the pod name with the name of your head pod
- you can find it by running ``kubectl -n ray get pods``.)

.. code-block:: shell

 $ kubectl -n ray exec example-cluster-ray-head-5455bb66c9-7l6xj -- python /home/ray/anaconda3/lib/python3.7/site-packages/ray/autoscaler/kubernetes/example_scripts/run_on_head.py


Alternatively, you can run tasks interactively on the cluster by connecting a remote
shell to one of the pods.

.. code-block:: shell

  # Get a remote shell to the head node.
  $ kubectl -n ray exec -it example-cluster-ray-head-5455bb66c9-7l6xj -- bash

  # Run the example program on the head node.
  root@ray-head-6f566446c-5rdmb:/# python /home/ray/anaconda3/lib/python3.7/site-packages/ray/autoscaler/kubernetes/example_scripts/run_on_head.py
  # You should see repeated output for 10 iterations and then 'Success!'


The program in this example uses ``ray.init(address="auto")`` to connect to the Ray cluster.

Accessing the Dashboard
-----------------------

The Ray Dashboard can accessed locally using ``kubectl port-forward``.

.. code-block:: shell

  $ kubectl -n ray port-forward service/example-cluster-ray-head 8265:8265

After running the above command locally, the Dashboard will be accessible at ``http://localhost:8265``.

You can also monitor the state of the cluster with ``kubectl logs`` when using the :ref:`Operator <operator-logs>` or with ``ray monitor`` when using
the :ref:`Ray Cluster Launcher <cluster-launcher-commands>`.

.. warning::
   The Dashboard currently shows resource limits of the physical host each Ray node is running on,
   rather than the limits of the container the node is running in.
   This is a known bug tracked `here <https://github.com/ray-project/ray/issues/11172>`_.


.. _k8s-comparison:

Cluster Launcher vs Operator
============================

We compare the Ray Cluster Launcher and Ray Kubernetes Operator as methods of managing an autoscaling Ray cluster.


Comparison of use cases
-----------------------

- The Cluster Launcher is convenient for development and experimentation. Using the Cluster Launcher requires a local installation of Ray. The Ray CLI then provides a convenient interface for interacting with a Ray cluster.

- The Operator is geared towards production use cases. It does not require installing Ray locally - all interactions with your Ray cluster are mediated by Kubernetes.


Comparison of architectures
---------------------------

- With the Cluster Launcher, the user launches a Ray cluster from their local environment by invoking ``ray up``. This provisions a pod for the Ray head node, which then runs the `autoscaling process <https://github.com/ray-project/ray/blob/master/python/ray/monitor.py>`__.

-  The `Operator <https://github.com/ray-project/ray/blob/master/python/ray/ray_operator/operator.py>`__ centralizes cluster launching and autoscaling in the `Operator pod <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml>`__. \
   The user creates a `Kubernetes Custom Resource`_ describing the intended state of the Ray cluster. \
   The Operator then detects the resource, launches a Ray cluster, and runs the autoscaling process in the operator pod. \
   The Operator can manage multiple Ray clusters by running an autoscaling process for each Ray cluster.

Comparison of configuration options
-----------------------------------

The configuration options for the two methods are completely analogous - compare sample configurations for the `Cluster Launcher <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes/example-full.yaml>`__
and for the `Operator <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml>`__.
With a few exceptions, the fields of the RayCluster resource managed by the Operator are camelCase versions of the corresponding snake_case Cluster Launcher fields.
In fact, the Operator `internally <https://github.com/ray-project/ray/blob/master/python/ray/ray_operator/operator_utils.py>`__ converts
RayCluster resources to Cluster Launching configs.

A summary of the configuration differences:

- The Cluster Launching field ``available_node_types`` for specifiying the types of pods available for autoscaling is renamed to ``podTypes`` in the Operator's RayCluster configuration.
- The Cluster Launching field ``resources`` for specifying custom Ray resources provided by a node type is renamed to ``rayResources`` in the Operator's RayCluster configuration.
- The ``provider`` field in the Cluster Launching config has no analogue in the Operator's RayCluster configuration. (The Operator fills this field internally.)
-  * When using the Cluster Launcher, ``head_ray_start_commands`` should include the argument ``--autoscaling-config=~/ray_bootstrap_config.yaml``; this is important for the configuration of the head node's autoscaler.
   * On the other hand, the Operator's ``headRayStartCommands`` should include a ``--no-monitor`` flag to prevent the autoscaling/monitoring process from running on the head node.

Questions or Issues?
--------------------

.. include:: /_help.rst



.. _`Kubernetes Job`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
.. _`Kubernetes Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`Kubernetes Operator`: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
.. _`Kubernetes Custom Resource`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _`Kubernetes Custom Resource Definition`: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
.. _`annotation`: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#attaching-metadata-to-objects
.. _`permissions`: https://kubernetes.io/docs/reference/access-authn-authz/rbac/
.. _`minikube`: https://minikube.sigs.k8s.io/docs/start/
.. _`namespaced`: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
