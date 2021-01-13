.. _ray-k8s-deploy:

Deploying on Kubernetes
=======================

.. note::

  This document is mainly for advanced Kubernetes usage. The easiest way to run a Ray cluster on Kubernetes is by using the built-in Cluster Launcher. Please see the :ref:`Cluster Launcher documentation <ray-launch-k8s>` for details.



This document assumes that you have access to a Kubernetes cluster and have
``kubectl`` installed locally and configured to access the cluster. It will
first walk you through how to deploy a Ray cluster on your existing Kubernetes
cluster, then explore a few different ways to run programs on the Ray cluster.


To learn about deploying an autoscaling Ray cluster using :ref:`Ray's Kubernetes operator<k8s-operator>`, read
:ref:`here<k8s-operator>`.

For information on using GPUs with Ray on Kubernetes, see :ref:`here<k8s-gpus>`.

The configuration ``yaml`` files used here are provided in the `Ray repository`_
as examples to get you started. When deploying real applications, you will probably
want to build and use your own container images, add more worker nodes to the
cluster (or use the `Kubernetes Horizontal Pod Autoscaler`_), and change the
resource requests for the head and worker nodes. Refer to the provided ``yaml``
files to be sure that you maintain important configuration options for Ray to
function properly.

.. _`Ray repository`: https://github.com/ray-project/ray/tree/master/doc/kubernetes

Creating a Ray Namespace
------------------------

First, create a `Kubernetes Namespace`_ for Ray resources on your cluster. The
following commands will create resources under this Namespace, so if you want
to use a different one than ``ray``, please be sure to also change the
`namespace` fields in the provided ``yaml`` files and anytime you see a ``-n``
flag passed to ``kubectl``.

.. code-block:: shell

  $ kubectl create -f ray/doc/kubernetes/ray-namespace.yaml

Starting a Ray Cluster
----------------------

.. toctree::
    :hidden:

    /cluster/k8s-operator.rst

A Ray cluster consists of a single head node and a set of worker nodes (the
provided ``ray-cluster.yaml`` file will start 3 worker nodes). In the example
Kubernetes configuration, this is implemented as:

- A ``ray-head`` `Kubernetes Service`_ that enables the worker nodes to discover the location of the head node on start up.
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

.. _ray-k8s-run:

Running Ray Programs
--------------------

This section assumes that you have a running Ray cluster (if you don't, please
refer to the section above to get started) and will walk you through three
different options to run a Ray program on it:

1. Using `kubectl exec` to run a Python script.
2. Using `kubectl exec -it bash` to work interactively in a remote shell.
3. Submitting a `Kubernetes Job`_.

Running a program using 'kubectl exec'
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run an example program that tests object transfers between nodes in the
cluster, try the following commands (don't forget to replace the head pod name
- you can find it by running ``kubectl -n ray get pods``):

.. code-block:: shell

  # Copy the test script onto the head node.
  $ kubectl -n ray cp ray/doc/kubernetes/example.py ray-head-5455bb66c9-7l6xj:/example.py

  # Run the example program on the head node.
  $ kubectl -n ray exec ray-head-5455bb66c9-7l6xj -- python example.py
  # You should see repeated output for 10 iterations and then 'Success!'

Running a program in a remote shell
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also run tasks interactively on the cluster by connecting a remote
shell to one of the pods.

.. code-block:: shell

  # Copy the test script onto the head node.
  $ kubectl -n ray cp ray/doc/kubernetes/example.py ray-head-5455bb66c9-7l6xj:/example.py

  # Get a remote shell to the head node.
  $ kubectl -n ray exec -it ray-head-5455bb66c9-7l6xj -- bash

  # Run the example program on the head node.
  root@ray-head-6f566446c-5rdmb:/# python example.py
  # You should see repeated output for 10 iterations and then 'Success!'

You can also start an IPython interpreter to work interactively:

.. code-block:: shell

  # From your local machine.
  $ kubectl -n ray exec -it ray-head-5455bb66c9-7l6xj -- ipython

  # From a remote shell on the head node.
  $ kubectl -n ray exec -it ray-head-5455bb66c9-7l6xj -- bash
  root@ray-head-6f566446c-5rdmb:/# ipython

Once you have the IPython interpreter running, try running the following example
program:

.. code-block:: python

  from collections import Counter
  import platform
  import time
  import ray

  ray.init(address="$RAY_HEAD_SERVICE_HOST:$RAY_HEAD_SERVICE_PORT_REDIS_PRIMARY")

  @ray.remote
  def f(x):
      time.sleep(0.01)
      return x + (platform.node(), )

  # Check that objects can be transferred from each node to each other node.
  %time Counter(ray.get([f.remote(f.remote(())) for _ in range(100)]))

Submitting a Job
~~~~~~~~~~~~~~~~

You can also submit a Ray application to run on the cluster as a `Kubernetes
Job`_. The Job will run a single pod running the Ray driver program to
completion, then terminate the pod but allow you to access the logs.

To submit a Job that downloads and executes an `example program`_ that tests
object transfers between nodes in the cluster, run the following command:

.. code-block:: shell

  $ kubectl create -f ray/doc/kubernetes/ray-job.yaml
  job.batch/ray-test-job-kw5gn created

.. _`example program`: https://github.com/ray-project/ray/blob/master/doc/kubernetes/example.py

To view the output of the Job, first find the name of the pod that ran it,
then fetch its logs:

.. code-block:: shell

  $ kubectl -n ray get pods
  NAME                          READY   STATUS      RESTARTS   AGE
  ray-head-5455bb66c9-7l6xj     1/1     Running     0          15s
  ray-test-job-kw5gn-5g7tv      0/1     Completed   0          10s
  ray-worker-5c49b7cc57-57tpv   1/1     Running     0          15s
  ray-worker-5c49b7cc57-6m4kp   1/1     Running     0          15s
  ray-worker-5c49b7cc57-jx2w2   1/1     Running     0          15s

  # Fetch the logs. You should see repeated output for 10 iterations and then
  # 'Success!'
  $ kubectl -n ray logs ray-test-job-kw5gn-5g7tv

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
  NAME                          READY   STATUS      RESTARTS   AGE
  ray-head-5455bb66c9-7l6xj     1/1     Running     0          60s
  ray-worker-5c49b7cc57-57tpv   1/1     Running     0          60s
  ray-worker-5c49b7cc57-6m4kp   1/1     Running     0          60s
  ray-worker-5c49b7cc57-jx2w2   1/1     Running     0          60s

Cleaning Up
-----------

To delete a running Ray cluster, you can run the following command:

.. code-block:: shell

  kubectl delete -f ray/doc/kubernetes/ray-cluster.yaml

.. _k8s-gpus:

Using GPUs
----------

To use GPUs on Kubernetes, you will need to configure both your Kubernetes setup and add additional values to your Ray cluster configuration.

For relevant documentation for GPU usage on different clouds, see instructions for `GKE`_, for `EKS`_, and for `AKS`_.

The `Ray Docker Hub <https://hub.docker.com/r/rayproject/>`_ hosts CUDA-based images packaged with Ray for use in Kubernetes pods.
For example, the image ``rayproject/ray-ml:nightly-gpu`` is ideal for running GPU-based ML workloads with the most recent nightly build of Ray.
Read :ref:`here<docker-images>` for further details on Ray images.

Using Nvidia GPUs requires specifying the relevant resource `limits` in the container fields of your Kubernetes configurations.
(Kubernetes `sets <https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/#using-device-plugins>`_
the GPU request equal to the limit.) The configuration for a pod running a Ray GPU image and
using one Nvidia GPU looks like this:

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
   generateName: example-cluster-ray-worker
   spec:
    ...
    containers:
     - name: ray-node
       image: rayproject/ray:nightly-gpu
       ...
       resources:
        cpu: 1000m
        memory: 512Mi
       limits:
        memory: 512Mi
        nvidia.com/gpu: 1

GPU taints and tolerations
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. note::

  Users using a managed Kubernetes service probably don't need to worry about this section.

The `Nvidia gpu plugin`_ for Kubernetes applies `taints`_ to GPU nodes; these taints prevent non-GPU pods from being scheduled on GPU nodes.
Managed Kubernetes services like GKE, EKS, and AKS automatically apply matching `tolerations`_
to pods requesting GPU resources. Tolerations are applied by means of Kubernetes's `ExtendedResourceToleration`_ `admission controller`_.
If this admission controller is not enabled for your Kubernetes cluster, you may need to manually add a GPU toleration each of to your GPU pod configurations. For example,

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
   generateName: example-cluster-ray-worker
   spec:
   ...
   tolerations:
   - effect: NoSchedule
     key: nvidia.com/gpu
     operator: Exists
   ...
   containers:
   - name: ray-node
     image: rayproject/ray:nightly-gpu
     ...

Further reference and discussion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read about Kubernetes device plugins `here <https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/>`__,
about Kubernetes GPU plugins `here <https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus>`__,
and about Nvidia's GPU plugin for Kubernetes `here <https://github.com/NVIDIA/k8s-device-plugin>`__.

If you run into problems setting up GPUs for your Ray cluster on Kubernetes, please reach out to us at `<https://discuss.ray.io>`_.

Questions or Issues?
--------------------

.. include:: /_help.rst


.. _`Kubernetes Horizontal Pod Autoscaler`: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
.. _`Kubernetes Namespace`: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
.. _`Kubernetes Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`Kubernetes Deployment`: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
.. _`Kubernetes Job`: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/

.. _`Discussion Board`: https://discuss.ray.io/
.. _`GKE`: https://cloud.google.com/kubernetes-engine/docs/how-to/gpus
.. _`EKS`: https://docs.aws.amazon.com/eks/latest/userguide/eks-optimized-ami.html
.. _`AKS`: https://docs.microsoft.com/en-us/azure/aks/gpu-cluster

.. _`tolerations`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`taints`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`Nvidia gpu plugin`: https://github.com/NVIDIA/k8s-device-plugin
.. _`admission controller`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/
.. _`ExtendedResourceToleration`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#extendedresourcetoleration
