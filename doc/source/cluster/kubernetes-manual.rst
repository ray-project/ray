:orphan:

.. include:: we_are_hiring.rst

.. _ray-k8s-static:

Deploying a Static Ray Cluster on Kubernetes
============================================

This document gives an example of how to manually deploy a non-autoscaling Ray cluster on Kubernetes.

- Learn about deploying an autoscaling Ray cluster using the :ref:`Ray Helm chart<ray-k8s-deploy>`.

Creating a Ray Namespace
------------------------

First, create a `Kubernetes Namespace`_ for Ray resources on your cluster. The
following commands will create resources under this Namespace, so if you want
to use a different one than ``ray``, please be sure to also change the
``namespace`` fields in the provided ``yaml`` files and anytime you see a ``-n``
flag passed to ``kubectl``.

.. code-block:: shell

  $ kubectl create namespace ray

Starting a Ray Cluster
----------------------


A Ray cluster consists of a single head node and a set of worker nodes (the
provided `ray-cluster.yaml <https://github.com/ray-project/ray/blob/master/doc/kubernetes/ray-cluster.yaml>`__ file will start 3 worker nodes). In the example
Kubernetes configuration, this is implemented as:

- A ``ray-head`` `Kubernetes Service`_ that enables the worker nodes to discover the location of the head node on start up.
  This Service also enables access to the Ray Client and Ray Dashboard.
- A ``ray-head`` `Kubernetes Deployment`_ that backs the ``ray-head`` Service with a single head node pod (replica).
- A ``ray-worker`` `Kubernetes Deployment`_ with multiple worker node pods (replicas) that connect to the ``ray-head`` pod using the ``ray-head`` Service.

Note that because the head and worker nodes are Deployments, Kubernetes will
automatically restart pods that crash to maintain the correct number of
replicas.

- If a worker node goes down, a replacement pod will be started and joined to the cluster.
- If the head node goes down, it will be restarted. This will start a new Ray cluster. Worker nodes that were connected to the old head node will crash and be restarted, connecting to the new head node when they come back up.

Try deploying a cluster with the provided Kubernetes config by running the
following command:

.. code-block:: shell

  $ kubectl apply -f ray/doc/kubernetes/ray-cluster.yaml

Verify that the pods are running by running ``kubectl get pods -n ray``. You
may have to wait up to a few minutes for the pods to enter the 'Running'
state on the first run.

.. code-block:: shell

  $ kubectl -n ray get pods
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-6bxvz     1/1     Running   0          10s
  ray-worker-5c49b7cc57-c6xs8   1/1     Running   0          5s
  ray-worker-5c49b7cc57-d9m86   1/1     Running   0          5s
  ray-worker-5c49b7cc57-kzk4s   1/1     Running   0          5s

.. note::

  You might see a nonzero number of RESTARTS for the worker pods. That can
  happen when the worker pods start up before the head pod and the workers
  aren't able to connect. This shouldn't affect the behavior of the cluster.

To change the number of worker nodes in the cluster, change the ``replicas``
field in the worker deployment configuration in that file and then re-apply
the config as follows:

.. code-block:: shell

  # Edit 'ray/doc/kubernetes/ray-cluster.yaml' and change the 'replicas'
  # field under the ray-worker deployment to, e.g., 4.

  # Re-apply the new configuration to the running deployment.
  $ kubectl apply -f ray/doc/kubernetes/ray-cluster.yaml
  service/ray-head unchanged
  deployment.apps/ray-head unchanged
  deployment.apps/ray-worker configured

  # Verify that there are now the correct number of worker pods running.
  $ kubectl -n ray get pods
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-6bxvz     1/1     Running   0          30s
  ray-worker-5c49b7cc57-c6xs8   1/1     Running   0          25s
  ray-worker-5c49b7cc57-d9m86   1/1     Running   0          25s
  ray-worker-5c49b7cc57-kzk4s   1/1     Running   0          25s
  ray-worker-5c49b7cc57-zzfg2   1/1     Running   0          0s

To validate that the restart behavior is working properly, try killing pods
and checking that they are restarted by Kubernetes:

.. code-block:: shell

  # Delete a worker pod.
  $ kubectl -n ray delete pod ray-worker-5c49b7cc57-c6xs8
  pod "ray-worker-5c49b7cc57-c6xs8" deleted

  # Check that a new worker pod was started (this may take a few seconds).
  $ kubectl -n ray get pods
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-6bxvz     1/1     Running   0          45s
  ray-worker-5c49b7cc57-d9m86   1/1     Running   0          40s
  ray-worker-5c49b7cc57-kzk4s   1/1     Running   0          40s
  ray-worker-5c49b7cc57-ypq8x   1/1     Running   0          0s

  # Delete the head pod.
  $ kubectl -n ray delete pod ray-head-5455bb66c9-6bxvz
  pod "ray-head-5455bb66c9-6bxvz" deleted

  # Check that a new head pod was started and the worker pods were restarted.
  $ kubectl -n ray get pods
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-gqzql     1/1     Running   0          0s
  ray-worker-5c49b7cc57-d9m86   1/1     Running   1          50s
  ray-worker-5c49b7cc57-kzk4s   1/1     Running   1          50s
  ray-worker-5c49b7cc57-ypq8x   1/1     Running   1          10s

  # You can even try deleting all of the pods in the Ray namespace and checking
  # that Kubernetes brings the right number back up.
  $ kubectl -n ray delete pods --all
  $ kubectl -n ray get pods
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-7l6xj     1/1     Running   0          10s
  ray-worker-5c49b7cc57-57tpv   1/1     Running   0          10s
  ray-worker-5c49b7cc57-6m4kp   1/1     Running   0          10s
  ray-worker-5c49b7cc57-jx2w2   1/1     Running   0          10s

Now that we have a running cluster, :ref:`we can execute Ray programs <ray-k8s-client>`.

Cleaning Up
-----------

To delete a running Ray cluster, you can run the following command:

.. code-block:: shell

  kubectl delete -f ray/doc/kubernetes/ray-cluster.yaml


Questions or Issues?
--------------------

.. include:: /_includes/_help.rst


.. _`Kubernetes Namespace`: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
.. _`Kubernetes Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`Kubernetes Deployment`: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
.. _`Kubernetes Job`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/

.. _`Discussion Board`: https://discuss.ray.io/
