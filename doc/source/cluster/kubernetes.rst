Deploying on Kubernetes
=======================

.. _ray-k8s-deploy:

Overview
--------
You can leverage your Kubernetes cluster as a substrate for execution of distributed Ray programs.
The :ref:`Ray Autoscaler<ray-cluster-overview>` spins up and deletes Kubernetes pods according to resource demands of the Ray workload - each Ray node runs in its own Kubernetes pod.

The Ray Kubernetes Operator
---------------------------
Deployments of Ray on Kubernetes are managed by the ``Ray Kubernetes Operator``.
The Ray Operator follows the standard Kubernetes `operator pattern`_: the main players are

  - A `Custom Resource`_ called a ``RayCluster``, which describes the desired state of the Ray cluster.
  - A `Custom Controller`_, the ``Ray Operator``, which processes ``RayCluster`` resources and manages the Ray cluster.

Under the hood, the Operator uses the `Ray Autoscaler<ray-cluster-overview>` to launch and scale your Ray cluster.

Installing the Ray Operator with Helm
-------------------------------------
Ray provides a `Helm`_ chart to simplify deployment of the Ray Operator and Ray clusters.

Currently, the `Ray Helm chart`_ is available on the the master branch of the Ray GitHub repo.
The chart will be published to a public Helm repo as part of an upcoming official Ray release.

Preparation
~~~~~~~~~~~

- Configure `kubectl`_ to access your Kubernetes cluster.
- Install `Helm 3`_
- Download the `Ray Helm chart`_.

To run the default example in this document, make sure your Kubernetes cluster can accomodate
additional resource requests of 4 CPU and 2.5Gi memory.

Installation
~~~~~~~~~~~~

You can install a small Ray cluster with a single ``helm`` command.

The default configuration consists of a Ray head pod and two worker pods,
with scaling allowed up to three workers.

.. code-block:: shell

  # Navigate to the directory containing the chart, e.g.
  $ cd ray/deploy/charts

  # Install a small Ray cluster with default configuration,
  # in a new namespace called "ray". Let's name the Helm release "example-cluster".
  $ helm -n ray install example-cluster --create-namespace ./ray

You can view the installed resources as follows.

.. code-block:: shell

  # The custom resource representing the state of the Ray cluster.
  $ kubectl -n get rayclusters

  # The Ray head node and two Ray worker nodes.
  $ kubectl -n ray get pods

  # A service exposing the Ray head node.
  $ kubectl -n ray get service

  # The operator deployment.
  # By default, the deployment is launched in namespace "default".
  $ kubectl get deployment ray-operator

  # The single pod of the operator deployment.
  $ kubectl get pod -l cluster.ray.io/component=operator

  # The `Custom Resource Definition`_ defining a RayCluster.
  $ kubectl get crd rayclusters.cluster.ray.io

Monitoring and observability
----------------------------

To view autoscaling logs, run a ``kubectl logs`` command on the operator pod:

.. code-block:: shell

  # The last 100 lines of logs.
  $ kubectl logs $(kubectl get pod -l cluster.ray.io/component=operator) | tail -n 100

The :ref:`Ray dashboard <ray-dashboard>`_ can be accessed on the Ray head node at port 8265.

.. code-block:: shell

  # Forward the relevant port from the service exposing the Ray head.
  $ kubectl -n ray port-forward service/example-cluster-ray-head 8265:8265

  # The dashboard can now be viewed in a browser at http://localhost:8265

Running Ray programs with Ray Client
------------------------------------

:ref:`Ray Client <ray-client>` can be used to execute Ray programs
on your Ray cluster. The Ray Client server runs on the Ray head node, on port 10001.

Using Ray Client to connect from outside the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One way to connect to the Ray cluster from outside your Kubernetes cluster
is to forward Ray Client server port:

.. code-block:: shell

  $ kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001

Then open a new shell and try out a sample program:

.. code-block:: shell

  $ python ray/doc/kubernetes/example_scripts/run_local_example.py

The program in this example uses ``ray.util.connect(127.0.0.1:10001)`` to connect to the Ray cluster.
The program for three Ray nodes to connect and then tests object transfer
between the nodes.

.. note::

  Connecting with Ray client requires using the matching minor versions of Python (for example 3.7)
  on the server and client end -- that is on the Ray head node and in the environment where
  ``ray.util.connect`` is invoked. Note that the default ``rayproject/ray`` images use Python 3.7.
  The latest offical Ray release builds are available for Python 3.6 and 3.8 at the `Ray Docker Hub <https://hub.docker.com/r/rayproject/ray/tags?page=1&ordering=last_updated&name=1.3.0>`_.

  Connecting with Ray client currently also requires matching Ray versions. In particular, to connect from a local machine to a cluster running the examples in this document, the :ref:`latest release version<installation>` of Ray must be installed locally.

Using Ray Client to connect from within the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can also connect to your Ray cluster from another pod in the same Kubernetes cluster.

For example, you can submit a Ray application to run on the Kubernetes cluster as a `Kubernetes
Job`_. The Job will run a single pod running the Ray driver program to
completion, then terminate the pod but allow you to access the logs.

The following command submits a Job which executes an `example Ray program`_.

.. code-block:: yaml

  $ kubectl -n ray create -f https://raw.githubusercontent.com/ray-project/ray/master/doc/kubernetes/job-example.yaml

The program executed by the job uses the name of the Ray cluster's head Service to connect:
``ray.util.connect("example-cluster-ray-head:10001")``.
The program waits for three Ray nodes to connect and then tests object transfer
between the nodes.

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

  # Cleanup
  $ kubectl -n ray delete job ray-test-job
- from :ref:`outside<ray-k8s-out>` the Kubernetes cluster (e.g. on your local machine)
- from :ref:`within <ray-k8s-in>` your Kubernetes cluster

Cleanup
-------

To remove a Ray Helm release and the associated API resources, use `helm uninstall`_.

.. code-block:: shell

  $ helm uninstall example-cluster

Note that this command `does not delete` the RayCluster CRD. If you wish to delete the CRD,
make sure all Ray Helm releases have been uninstalled, then run ``kubectl delete crd rayclusters.cluster.ray.io``.

Next steps
----------
For further details and advanced configuration, see

- :ref:`Ray Operator and Helm Chart Configuration<k8s-advanced>`





















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

The Ray Kubernetes operator works by managing a user-submitted `Kubernetes Custom Resource`_ (CR) called a ``RayCluster``.
A RayCluster custom resource describes the desired state of the Ray cluster.
[All the key words in this section]
Operator manages CRD.
For convenient deployment of the Ray Operator and CRDs ``Helm`` Chart.

The Ray Helm chart is currently available. Accessible from the Ray repository.
Helm repository in .


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
The :ref:`Ray Cluster Launcher <cluster-cloud>` is geared towards experimentation and development and can be used to launch Ray clusters on Kubernetes (among other backends).
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

.. note::

  The configuration ``yaml`` files used in this document are provided in the `Ray repository`_
  as examples to get you started. When deploying real applications, you will probably
  want to build and use your own container images, add more worker nodes to the
  cluster, and change the resource requests for the head and worker nodes. Refer to the provided ``yaml``
  files to be sure that you maintain important configuration options for Ray to
  function properly.


.. _`Ray repository`: https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/kubernetes

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


Installing the Operator
-----------------------
For this example, let's install...
``helm -n ray install example-cluster --create-namespace ./ray``


Starting the Operator
---------------------

Set-up for the Ray Operator consists of three steps --
:ref:`applying a CRD <apply-crd>`, :ref:`picking a namespace <operator-namespace>`, and :ref:`launching the Operator Pod <operator-pod-launch>`.

.. _apply-crd:

(1) Applying the RayCluster Custom Resource Definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Ray Kubernetes operator works by managing a user-submitted `Kubernetes Custom Resource`_ (CR) called a ``RayCluster``.
A RayCluster custom resource describes the desired state of the Ray cluster.

To get started, we need to apply the `Kubernetes Custom Resource Definition`_ (CRD) defining a RayCluster.


.. code-block:: shell

 $ kubectl apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml

 customresourcedefinition.apiextensions.k8s.io/rayclusters.cluster.ray.io created

.. note::

    The file ``cluster_crd.yaml`` defining the CRD is not meant to meant to be modified by the user. Rather, users :ref:`configure <operator-launch>` a RayCluster CR via a file like `ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml>`__.
    The Kubernetes API server then validates the user-submitted RayCluster resource against the CRD.

.. _operator-namespace:

(2) Picking a Kubernetes Namespace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We will launch Ray clusters within a Kubernetes `namespace`_.
You can use an existing namespace for your Ray clusters or create a new one if you have permissions.
For this example, we will create a namespace called ``ray``.

.. code-block:: shell

 $ kubectl create namespace ray

 namespace/ray created

.. _operator-pod-launch:

(3) Launching the Operator Pod
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray provides both namespaced and cluster-scoped Operators.

- The namespaced Operator manages all Ray clusters within a single Kubernetes namespace.
- The cluster-scoped Operator manages all Ray clusters, across all namespaces of your Kubernetes cluster.

Choose the option that suits your needs.
However, do not simultaneously run namespaced and cluster-scoped Ray Operators within one Kubernetes cluster, as this will lead to unintended effects.

.. tabs::
   .. group-tab:: Namespaced Operator

        .. code-block:: shell

         $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator_namespaced.yaml

         serviceaccount/ray-operator-serviceaccount created
         role.rbac.authorization.k8s.io/ray-operator-role created
         rolebinding.rbac.authorization.k8s.io/ray-operator-rolebinding created
         deployment.apps/ray-operator created

   .. group-tab:: Cluster-scoped Operator

        .. code-block:: shell

         $ kubectl apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator_cluster_scoped.yaml

         serviceaccount/ray-operator-serviceaccount created
         clusterrole.rbac.authorization.k8s.io/ray-operator-clusterrole created
         clusterrolebinding.rbac.authorization.k8s.io/ray-operator-clusterrolebinding created
         deployment.apps/ray-operator created

The output shows that we've launched a `Deployment`_ named ``ray-operator``.
This Deployment maintains one Pod, which runs the operator process.
The ServiceAccount, Role, and RoleBinding we have created grant the operator pod the `permissions`_ it needs to manage Ray clusters.


.. _operator-launch:

Launching Ray Clusters
----------------------
Having set up the Operator, we can now launch Ray clusters. To launch a Ray cluster, we create a RayCluster custom resource.

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
 ray-operator-d5f655d9-lhspx        1/1     Running   0          2m33s

We see four pods: the operator pod, the Ray head node, and two Ray worker nodes.

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
 ray-operator-d5f655d9-lhspx         1/1     Running   0          10m

Now we can :ref:`run Ray programs<ray-k8s-run>` on our Ray clusters.

.. _operator-logs:

Monitoring
----------
Autoscaling logs are written to the operator pod's ``stdout`` and can be accessed with :code:`kubectl logs`.
Each line of output is prefixed by a string of form :code:`<cluster name>,<namespace>:` .
The following command gets the last hundred lines of autoscaling logs for our second cluster. (Be sure to substitute the name of your operator pod from the above ``get pods`` command.)

.. tabs::
   .. group-tab:: Namespaced Operator

      .. code-block:: shell

       $ kubectl -n ray logs ray-operator-d5f655d9-lhspx | grep ^example-cluster2,ray: | tail -n 100

   .. group-tab:: Cluster-scoped Operator

      .. code-block:: shell

       $ kubectl logs ray-operator-d5f655d9-lhspx | grep ^example-cluster2,ray: | tail -n 100

The output should include monitoring updates that look like this:

.. code-block:: shell

    example-cluster2,ray:2020-12-12 13:55:36,814        DEBUG autoscaler.py:693 -- Cluster status: 1 nodes
    example-cluster2,ray: - MostDelayedHeartbeats: {'172.17.0.4': 0.04093289375305176, '172.17.0.5': 0.04084634780883789}
    example-cluster2,ray: - NodeIdleSeconds: Min=36 Mean=38 Max=41
    example-cluster2,ray: - ResourceUsage: 0.0/2.0 CPU, 0.0/1.0 Custom1, 0.0/1.0 is_spot, 0.0 GiB/0.58 GiB memory, 0.0 GiB/0.1 GiB object_store_memory
    example-cluster2,ray: - TimeSinceLastHeartbeat: Min=0 Mean=0 Max=0
    example-cluster2,ray:Worker node types:
    example-cluster2,ray: - worker-nodes: 1
    example-cluster2,ray:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:148 -- Cluster resources: [{'object_store_memory': 1.0, 'node:172.17.0.4': 1.0, 'memory': 5.0, 'CPU': 1.0}, {'object_store_memory': 1.0, 'is_spot': 1.0, 'memory': 6.0, 'node:172.17.0.5': 1.0, 'Custom1': 1.0, 'CPU': 1.0}]
    example-cluster2:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:149 -- Node counts: defaultdict(<class 'int'>, {'head-node': 1, 'worker-nodes
    ': 1})
    example-cluster2,ray:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:159 -- Placement group demands: []
    example-cluster2,ray:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:186 -- Resource demands: []
    example-cluster2,ray:2020-12-12 13:55:36,870        INFO resource_demand_scheduler.py:187 -- Unfulfilled demands: []
    example-cluster2,ray:2020-12-12 13:55:36,891        INFO resource_demand_scheduler.py:209 -- Node requests: {}
    example-cluster2,ray:2020-12-12 13:55:36,903        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-tdxdr is not being updated and passes config check (can_update=True).
    example-cluster2:2020-12-12 13:55:36,923        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-tdxdr is not being updated and passes config check (can_update=True).

Cleaning Up
-----------
We shut down a Ray cluster by deleting the associated RayCluster resource.
Either of the next two commands will delete our second cluster ``example-cluster2``.

.. code-block:: shell

 $ kubectl -n ray delete raycluster example-cluster2
 # OR
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

The pods associated with ``example-cluster2`` are marked for termination. In a few moments, we check that these pods are gone:

.. code-block:: shell

 $ kubectl -n ray get pods
 NAME                               READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-th4wv     1/1     Running   0          57m
 example-cluster-ray-worker-q9pjn   1/1     Running   0          56m
 example-cluster-ray-worker-qltnp   1/1     Running   0          56m
 ray-operator-d5f655d9-lhspx        1/1     Running   0          57m

Only the operator pod and the first ``example-cluster`` remain.

Next, we delete ``example-cluster``:

.. code-block:: shell

 $ kubectl -n ray delete raycluster example-cluster

Finally, we delete the operator:

.. tabs::
   .. group-tab:: Namespaced Operator

        .. code-block:: shell

         $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator_namespaced.yaml

   .. group-tab:: Cluster-scoped Operator

        .. code-block:: shell

         $ kubectl delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator_cluster_scoped.yaml


.. _ray-k8s-interact:

Interacting with a Ray Cluster
==============================
:ref:`Ray Client <ray-client>` allows you to connect to your Ray cluster on Kubernetes and execute Ray programs.
The Ray Client server runs on the Ray head node, on port 10001.

:ref:`Ray Dashboard <ray-dashboard>` gives visibility into the state of your cluster.
The dashboard uses port 8265 on the Ray head node.

The Ray Operator automatically configures a `Kubernetes Service`_ targeting the relevant ports on the head node.
After launching a Ray cluster, you can view the configured service:

.. code-block:: shell

 $ kubectl -n ray get services

  NAME                       TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)              AGE
  example-cluster-ray-head   ClusterIP   10.106.123.159   <none>        10001/TCP,8265/TCP   52s

.. _ray-k8s-run:

Running Ray Programs
--------------------

Ray Client can be used to submit Ray programs
- from :ref:`within <_ray-k8s-in>` your Kubernetes cluster
- from :ref:`outside <_ray-k8s-out>` the Kubernetes cluster (e.g. on your local machine)

.. _ray-k8s-in:

Using Ray Client to connect from within the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can connect to your Ray cluster from another pod in the same Kubernetes cluster.

For example, you can submit a Ray application to run on the Kubernetes cluster as a `Kubernetes
Job`_. The Job will run a single pod running the Ray driver program to
completion, then terminate the pod but allow you to access the logs.

The following command submits a Job which executes an `example Ray program`_.

.. code-block:: yaml

  $ kubectl -n ray create -f ray/doc/kubernetes/job-example.yaml

The program executed by the Job waits for three Ray nodes to connect and then tests object transfer
between the nodes.

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

.. _`example Ray program`: https://github.com/ray-project/ray/blob/master/doc/kubernetes/example_scripts/job_example.py

.. _ray-k8s-out:

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

  $ python ray/doc/kubernetes/example_scripts/run_local_example.py

The program in this example uses ``ray.util.connect(127.0.0.1:10001)`` to connect to the Ray cluster.

.. note::

  Connecting with Ray client requires using the matching minor versions of Python (for example 3.7)
  on the server and client end -- that is on the Ray head node and in the environment where
  ``ray.util.connect`` is invoked. Note that the default ``rayproject/ray`` images use Python 3.7.
  The latest offical Ray release builds are available for Python 3.6 and 3.8 at the `Ray Docker Hub <https://hub.docker.com/r/rayproject/ray/tags?page=1&ordering=last_updated&name=1.3.0>`_.

  Connecting with Ray client currently also requires matching Ray versions. In particular, to connect from a local machine to a cluster running the examples in this document, the :ref:`latest release version<installation>` of Ray must be installed locally.


Accessing the Dashboard
-----------------------

The Ray Dashboard can be accessed locally using ``kubectl port-forward``.

.. code-block:: shell

  $ kubectl -n ray port-forward service/example-cluster-ray-head 8265:8265

After running the above command locally, the Dashboard will be accessible at ``http://localhost:8265``.

You can also monitor the state of the cluster with ``kubectl logs`` when using the :ref:`Operator <operator-logs>` or with ``ray monitor`` when using
the :ref:`Ray Cluster Launcher <cluster-launcher-commands>`.

.. note::

  ``helm uninstall`` will not delete the RayCluster CRD. If you wish to delete the CRD, first make sure all Ray Helm releases have been uninstalled, then run ``kubectl delete crd rayclusters.cluster.ray.io``.

Questions or Issues?
--------------------

.. include:: /_help.rst



.. _`Kubernetes Job`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
.. _`Kubernetes Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`operator pattern`: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
.. _`Kubernetes Custom Resource`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _`Kubernetes Custom Resource Definition`: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
.. _`annotation`: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#attaching-metadata-to-objects
.. _`permissions`: https://kubernetes.io/docs/reference/access-authn-authz/rbac/
.. _`minikube`: https://minikube.sigs.k8s.io/docs/start/
.. _`namespace`: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
.. _`Deployment`: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
.. _`Ray Helm chart`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/
.. _`kubectl`: https://kubernetes.io/docs/tasks/tools/
.. _`Helm 3`: https://helm.sh/
.. _`Helm`: https://helm.sh/
.. _`helm uninstall`: https://helm.sh/docs/helm/helm_uninstall/
.. _`does not delete`: https://helm.sh/docs/chart_best_practices/custom_resource_definitions/
