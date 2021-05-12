Deploying on Kubernetes
=======================

.. _ray-k8s-deploy:

Overview
--------
You can leverage your Kubernetes cluster as a substrate for execution of distributed Ray programs.
The :ref:`Ray Autoscaler<ray-cluster-overview>` spins up and deletes Kubernetes pods according to resource demands of the Ray workload - each Ray node runs in its own Kubernetes `Pod`_.

Quick Guide
-----------

This document cover the following topics:

- :ref:`Intro to Ray Kubernetes Operator<ray-operator>`
- :ref:`Launching Ray clusters with the Ray Helm Chart<ray-helm>`
- :ref:`Monitoring Ray clusters<ray-k8s-monitor>`
- :ref:`Running Ray programs using Ray Client<ray-k8s-client>`

You can find more information at the following links:

- :ref:`Ray Operator and Helm chart configuration<k8s-advanced>`
- :ref:`GPU usage with Kubernetes<k8s-gpus>`
- :ref:`Using Ray Tune on your Kubernetes cluster<tune-kubernetes>`
- :ref:`How to manually set up a non-autoscaling Ray cluster on Kubernetes<ray-k8s-static>`

.. _ray-operator:

The Ray Kubernetes Operator
---------------------------
Deployments of Ray on Kubernetes are managed by the ``Ray Kubernetes Operator``.
The Ray Operator follows the standard Kubernetes `operator pattern`_: the main players are

  - A `Custom Resource`_ called a ``RayCluster``, which describes the desired state of the Ray cluster.
  - A `Custom Controller`_, the ``Ray Operator``, which processes ``RayCluster`` resources and manages the Ray cluster.

Under the hood, the Operator uses the `Ray Autoscaler<ray-cluster-overview>` to launch and scale your Ray cluster.

The rest of this document explains how to launch a small example Ray cluster on Kubernetes.
See :ref:`here<k8s-advanced>` for further configuration details.

.. _ray-helm:

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

.. _ray-k8s-monitor:

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

.. _ray-k8s-client:

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

Cleanup
-------

To remove a Ray Helm release and the associated API resources, use `helm uninstall`_.

.. code-block:: shell

  # Delete the Ray release.
  $ helm uninstall example-cluster

  # Optionally, delete the namespace created for our Ray release.
  $ kubectl delete namespace ray

Note that ``helm uninstall`` `does not delete`_ the RayCluster CRD. If you wish to delete the CRD,
make sure all Ray Helm releases have been uninstalled, then run ``kubectl delete crd rayclusters.cluster.ray.io``.

Next steps
----------
For further details and advanced configuration, see
- :ref:`Ray Operator and Helm Chart Configuration<k8s-advanced>`


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
.. _`Pod`: https://kubernetes.io/docs/concepts/workloads/pods/
