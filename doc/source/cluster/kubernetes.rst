.. include:: we_are_hiring.rst

.. _ray-k8s-deploy:

Deploying on Kubernetes
=======================

Overview
--------
You can leverage your `Kubernetes`_ cluster as a substrate for execution of distributed Ray programs.
The :ref:`Ray Autoscaler<cluster-index>` spins up and deletes Kubernetes `Pods`_ according to the resource demands of the Ray workload. Each Ray node runs in its own Kubernetes Pod.

Quick Guide
-----------

This document cover the following topics:

- :ref:`Intro to the Ray Kubernetes Operator<ray-operator>`
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
The Ray Operator follows the standard Kubernetes `Operator pattern`_. The main players are

- A `Custom Resource`_ called a ``RayCluster``, which describes the desired state of the Ray cluster.
- A `Custom Controller`_, the ``Ray Operator``, which processes ``RayCluster`` resources and manages the Ray cluster.

Under the hood, the Operator uses the :ref:`Ray Autoscaler<cluster-index>` to launch and scale your Ray cluster.

The rest of this document explains how to launch a small example Ray cluster on Kubernetes.

- :ref:`Ray on Kubernetes Configuration and Advanced Usage<k8s-advanced>`.

.. _ray-helm:

Installing the Ray Operator with Helm
-------------------------------------
Ray provides a `Helm`_ chart to simplify deployment of the Ray Operator and Ray clusters.

The `Ray Helm chart`_ is available as part of the Ray GitHub repository.
The chart will be published to a public Helm repository as part of a future Ray release.

Preparation
~~~~~~~~~~~

- Configure `kubectl`_ to access your Kubernetes cluster.
- Install `Helm 3`_.
- Download the `Ray Helm chart`_.

To run the default example in this document, make sure your Kubernetes cluster can accomodate
additional resource requests of 4 CPU and 2.5Gi memory.

Installation
~~~~~~~~~~~~

You can install a small Ray cluster with a single ``helm`` command.
The default cluster configuration consists of a Ray head pod and two worker pods,
with scaling allowed up to three workers.

.. code-block:: shell

  # Navigate to the directory containing the chart
  $ cd ray/deploy/charts

  # Install a small Ray cluster with the default configuration
  # in a new namespace called "ray". Let's name the Helm release "example-cluster."
  $ helm -n ray install example-cluster --create-namespace ./ray
  NAME: example-cluster
  LAST DEPLOYED: Fri May 14 11:44:06 2021
  NAMESPACE: ray
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None

View the installed resources as follows.

.. code-block:: shell

  # The custom resource representing the state of the Ray cluster.
  $ kubectl -n ray get rayclusters
  NAME              STATUS    RESTARTS   AGE
  example-cluster   Running   0          53s

  # The Ray head node and two Ray worker nodes.
  $ kubectl -n ray get pods
  NAME                                    READY   STATUS    RESTARTS   AGE
  example-cluster-ray-head-type-5926k     1/1     Running   0          57s
  example-cluster-ray-worker-type-8gbwx   1/1     Running   0          40s
  example-cluster-ray-worker-type-l6cvx   1/1     Running   0          40s

  # A service exposing the Ray head node.
  $ kubectl -n ray get service
  NAME                       TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                       AGE
  example-cluster-ray-head   ClusterIP   10.8.11.17   <none>        10001/TCP,8265/TCP,8000/TCP   115s

  # The operator deployment.
  # By default, the deployment is launched in namespace "default".
  $ kubectl get deployment ray-operator
  NAME           READY   UP-TO-DATE   AVAILABLE   AGE
  ray-operator   1/1     1            1           3m1s

  # The single pod of the operator deployment.
  $ kubectl get pod -l cluster.ray.io/component=operator
  NAME                            READY   STATUS    RESTARTS   AGE
  ray-operator-84f5d57b7f-xkvtm   1/1     Running   0          3m35

  # The Custom Resource Definition defining a RayCluster.
  $ kubectl get crd rayclusters.cluster.ray.io
  NAME                         CREATED AT
  rayclusters.cluster.ray.io   2021-05-14T18:44:02

.. _ray-k8s-monitor:

Observability
-------------

To view autoscaling logs, run a ``kubectl logs`` command on the operator pod:

.. code-block:: shell

  # The last 100 lines of logs.
  $ kubectl logs \
    $(kubectl get pod -l cluster.ray.io/component=operator -o custom-columns=:metadata.name) \
    | tail -n 100

.. _ray-k8s-dashboard:

The :ref:`Ray dashboard<ray-dashboard>` can be accessed on the Ray head node at port ``8265``.

.. code-block:: shell

  # Forward the relevant port from the service exposing the Ray head.
  $ kubectl -n ray port-forward service/example-cluster-ray-head 8265:8265

  # The dashboard can now be viewed in a browser at http://localhost:8265

.. _ray-k8s-client:

Running Ray programs with Ray Jobs Submission
---------------------------------------------

:ref:`Ray Job Submission <jobs-overview>` can be used to submit Ray programs to your Ray cluster.
To do this, you must be able to access the Ray Dashboard, which runs on the Ray head node on port ``8265``.
One way to do this is to port forward ``127.0.0.1:8265`` on your local machine to ``127.0.0.1:8265`` on the head node using the :ref:`Kubernetes port-forwarding command<ray-k8s-dashboard>`.

.. code-block:: bash

  $ kubectl -n ray port-forward service/example-cluster-ray-head 8265:8265

Then in a new shell, you can run a job using the CLI:

.. code-block:: bash

  $ export RAY_ADDRESS="http://127.0.0.1:8265"

  $ ray job submit --runtime-env-json='{"working_dir": "./", "pip": ["requests==2.26.0"]}' -- "python script.py"
  2021-12-01 23:04:52,672 INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
  2021-12-01 23:04:52,809 INFO sdk.py:144 -- Uploading package gcs://_ray_pkg_bbcc8ca7e83b4dc0.zip.
  2021-12-01 23:04:52,810 INFO packaging.py:352 -- Creating a file package for local directory './'.
  2021-12-01 23:04:52,878 INFO cli.py:105 -- Job submitted successfully: raysubmit_RXhvSyEPbxhcXtm6.
  2021-12-01 23:04:52,878 INFO cli.py:106 -- Query the status of the job using: `ray job status raysubmit_RXhvSyEPbxhcXtm6`.

For more ways to run jobs, including a Python SDK and a REST API, see :ref:`Ray Job Submission <jobs-overview>`.



Running Ray programs with Ray Client
------------------------------------

:ref:`Ray Client <ray-client>` can be used to interactively execute Ray programs on your Ray cluster. The Ray Client server runs on the Ray head node, on port ``10001``.

.. note::

  Connecting with Ray client requires using matching minor versions of Python (for example 3.7)
  on the server and client end, that is, on the Ray head node and in the environment where
  ``ray.init("ray://<host>:<port>")`` is invoked. Note that the default ``rayproject/ray`` images use Python 3.7.
  The latest offical Ray release builds are available for Python 3.6 and 3.8 at the `Ray Docker Hub <https://hub.docker.com/r/rayproject/ray>`_.

  Connecting with Ray client also requires matching Ray versions. To connect from a local machine to a cluster running the examples in this document, the :ref:`latest release version<installation>` of Ray must be installed locally.

Using Ray Client to connect from outside the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
One way to connect to the Ray cluster from outside your Kubernetes cluster
is to forward the Ray Client server port:

.. code-block:: shell

  $ kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001

Then open a new shell and try out a `sample Ray program`_:

.. code-block:: shell

  $ python ray/doc/kubernetes/example_scripts/run_local_example.py

The program in this example uses ``ray.init("ray://127.0.0.1:10001")`` to connect to the Ray cluster.
The program waits for three Ray nodes to connect and then tests object transfer
between the nodes.


Using Ray Client to connect from within the Kubernetes cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can also connect to your Ray cluster from another pod in the same Kubernetes cluster.

For example, you can submit a Ray application to run on the Kubernetes cluster as a `Kubernetes
Job`_. The Job will run a single pod running the Ray driver program to
completion, then terminate the pod but allow you to access the logs.

The following command submits a Job which executes an `example Ray program`_.

.. code-block:: yaml

  $ kubectl -n ray create -f https://raw.githubusercontent.com/ray-project/ray/master/doc/kubernetes/job-example.yaml
  job.batch/ray-test-job created

The program executed by the job uses the name of the Ray cluster's head Service to connect:
``ray.init("ray://example-cluster-ray-head:10001")``.
The program waits for three Ray nodes to connect and then tests object transfer
between the nodes.

To view the output of the Job, first find the name of the pod that ran it,
then fetch its logs:

.. code-block:: shell

  $ kubectl -n ray get pods
  NAME                                    READY   STATUS    RESTARTS   AGE
  example-cluster-ray-head-type-5926k     1/1     Running   0          21m
  example-cluster-ray-worker-type-8gbwx   1/1     Running   0          21m
  example-cluster-ray-worker-type-l6cvx   1/1     Running   0          21m
  ray-test-job-dl9fv                      1/1     Running   0          3s

  # Fetch the logs. You should see repeated output for 10 iterations and then
  # 'Success!'
  $ kubectl -n ray logs ray-test-job-dl9fv

  # Cleanup
  $ kubectl -n ray delete job ray-test-job
  job.batch "ray-test-job" deleted

.. tip::

  Code dependencies for a given Ray task or actor must be installed on each Ray node that might run the task or actor.
  Typically, this means that all Ray nodes need to have the same dependencies installed.
  To achieve this, you can build a custom container image, using one of the `official Ray images <https://hub.docker.com/r/rayproject/ray>`_ as the base.
  Alternatively, try out the experimental :ref:`Runtime Environments<runtime-environments>` API (latest Ray release version recommended.)

.. _k8s-cleanup-basic:

Cleanup
-------

To remove a Ray Helm release and the associated API resources, use `kubectl delete`_ and `helm uninstall`_.
Note the order of the commands below.

.. code-block:: shell

  # First, delete the RayCluster custom resource.
  $ kubectl -n ray delete raycluster example-cluster
  raycluster.cluster.ray.io "example-cluster" deleted

  # Delete the Ray release.
  $ helm -n ray uninstall example-cluster
  release "example-cluster" uninstalled

  # Optionally, delete the namespace created for our Ray release.
  $ kubectl delete namespace ray
  namespace "ray" deleted

Note that ``helm uninstall`` `does not delete`_ the RayCluster CRD. If you wish to delete the CRD,
make sure all Ray Helm releases have been uninstalled, then run ``kubectl delete crd rayclusters.cluster.ray.io``.

- :ref:`More details on resource cleanup<k8s-cleanup>`

Next steps
----------
:ref:`Ray Operator Advanced Configuration<k8s-advanced>`

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst

.. _`Kubernetes`: https://kubernetes.io/
.. _`Kubernetes Job`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
.. _`Kubernetes Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`operator pattern`: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
.. _`Custom Resource`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _`Custom Controller`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#custom-controllers
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
.. _`kubectl delete`: https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#delete
.. _`helm uninstall`: https://helm.sh/docs/helm/helm_uninstall/
.. _`does not delete`: https://helm.sh/docs/chart_best_practices/custom_resource_definitions/
.. _`Pods`: https://kubernetes.io/docs/concepts/workloads/pods/
.. _`example Ray program`: https://github.com/ray-project/ray/tree/master/doc/kubernetes/example_scripts/job_example.py
.. _`sample Ray program`: https://github.com/ray-project/ray/tree/master/doc/kubernetes/example_scripts/run_local_example.py
.. _`official Ray images`: https://hub.docker.com/r/rayproject/ray
.. _`Ray Docker Hub`: https://hub.docker.com/r/rayproject/ray
