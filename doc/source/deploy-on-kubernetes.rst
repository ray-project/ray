Deploying on Kubernetes
=======================

.. warning::

  These instructions have not been tested extensively. If you have a suggestion
  for how to improve them, please open a pull request or email
  ray-dev@googlegroups.com.

You can run Ray on top of Kubernetes. This document assumes that you have access
to a Kubernetes cluster and have ``kubectl`` installed locally.

Start by cloning the Ray repository.

.. code-block:: shell

  git clone https://github.com/ray-project/ray.git

Work Interactively on the Cluster
---------------------------------

To work interactively, first start Ray on Kubernetes.

.. code-block:: shell

  kubectl create -f ray/kubernetes/head.yaml
  kubectl create -f ray/kubernetes/worker.yaml

This will start one head pod and 3 worker pods. You can check that the pods are
running by running ``kubectl get pods -n ray``.

You should see something like the following (you will have to wait a couple
minutes for the pods to enter the "Running" state).

.. code-block:: shell

  $ kubectl get pods -n ray
  NAME                          READY   STATUS    RESTARTS   AGE
  ray-head-5455bb66c9-6bxvz     1/1     Running   0          10s
  ray-worker-5c49b7cc57-c6xs8   1/1     Running   0          5s
  ray-worker-5c49b7cc57-d9m86   1/1     Running   0          5s
  ray-worker-5c49b7cc57-kzk4s   1/1     Running   0          5s

To run tasks interactively on the cluster, connect to one of the pods, e.g.,

.. code-block:: shell

  kubectl exec -it -n ray ray-head-5455bb66c9-6bxvz -- bash

Start an IPython interpreter, e.g., ``ipython``

.. code-block:: python

  from collections import Counter
  import time
  import ray

  # Note that if you run this script on a non-head node, then you must replace
  # "localhost" with socket.gethostbyname("ray-head").
  ray.init(redis_address="localhost:6379")

  @ray.remote
  def f(x):
      time.sleep(0.01)
      return x + (ray.services.get_node_ip_address(), )

  # Check that objects can be transferred from each node to each other node.
  %time Counter(ray.get([f.remote(f.remote(())) for _ in range(1000)]))

Submitting a Script to the Cluster
----------------------------------

To submit a self-contained Ray application to your Kubernetes cluster, do the
following.

.. code-block:: shell

  kubectl create -f ray/kubernetes/submit.yaml

One of the pods will download and run `this example script`_.

.. _`this example script`: https://github.com/ray-project/ray/blob/master/doc/kubernetes/example.py

The script prints its output. To view the output, first find the pod name by
running ``kubectl get all``. You'll see output like the following.

.. code-block:: shell

  $ kubectl get all -n ray
  NAME                              READY   STATUS    RESTARTS   AGE
  pod/ray-head-5486648dc9-c6hz2     1/1     Running   0          11s
  pod/ray-worker-5c49b7cc57-2jz4l   1/1     Running   0          11s
  pod/ray-worker-5c49b7cc57-8nwjk   1/1     Running   0          11s
  pod/ray-worker-5c49b7cc57-xlksn   1/1     Running   0          11s

  NAME                 TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                          AGE
  service/ray-head     ClusterIP   10.110.54.241   <none>        6379/TCP,6380/TCP,6381/TCP,12345/TCP,12346/TCP   11s

  NAME                         READY   UP-TO-DATE   AVAILABLE   AGE
  deployment.apps/ray-head     1/1     1            1           11s
  deployment.apps/ray-worker   3/3     3            3           11s

  NAME                                    DESIRED   CURRENT   READY   AGE
  replicaset.apps/ray-head-5486648dc9     1         1         1       11s
  replicaset.apps/ray-worker-5c49b7cc57   3         3         3       11s

Find the name of the ``ray-head`` pod and run the equivalent of

.. code-block:: shell

  kubectl logs ray-head-5486648dc9-c6hz2 -n ray

Cleaning Up
-----------

To remove the services you have created, run the following.

.. code-block:: shell

  kubectl delete -n ray service/ray-head \
                        deployment.apps/ray-head \
                        deployment.apps/ray-worker


Customization
-------------

You will probably need to do some amount of customization.

1. The example above uses the Docker image ``rayproject/examples``, which is
   built using `these Dockerfiles`_. You will most likely need to use your own
   Docker image.
2. You will need to modify the ``command`` and ``args`` fields to potentially
   install and run the script of your choice.
3. You will need to customize the resource requests.

TODO
----

The following are also important but haven't been documented yet. Contributions
are welcome!

1. Request CPU/GPU/memory resources.
2. Increase shared memory.
3. How to make Kubernetes clean itself up once the script finishes.
4. Follow Kubernetes best practices.

.. _`these Dockerfiles`: https://github.com/ray-project/ray/tree/master/docker
