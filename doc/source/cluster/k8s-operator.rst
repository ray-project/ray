.. _k8s_operator:

The Ray Kubernetes Operator
=================================

Ray provides a `Kubernetes Operator`_ for managing autoscaling Ray clusters.
Using the operator provides similar functionality to deploying a Ray cluster using
the :ref:`Ray Cluster Launcher<ref-autoscaling>`. However, working with the operator does not require
running Ray locally -- all interactions with your Ray cluster are mediated by Kubernetes.

The operator makes use of a `Kubernetes Custom Resource`_ called a *RayCluster*.
A RayCluster is specified by a configuration similar to the cluster configuration files used by the Ray Cluster Launcher. 
Internally, the operator uses Ray's autoscaler to manage your Ray cluster. However, the autoscaler runs in a
separate operator pod, rather than on the Ray head node. Applying multiple RayCluster custom resources in the operator's 
namespace allows the operator to manage several Ray clusters. 

The rest of this document explains step-by-step how to use the Ray Kubernetes Operator to launch a Ray cluster on your existing Kubernetes cluster.


Applying the RayCluster Custom Resource Definition
--------------------------------------------------
First, we need to apply the `Kubernetes Custom Resource Definition`_ (CRD) defining a RayCluster.

.. note::

    Creating a Custom Resource Definition requires the appropriate Kubernetes cluster-level privileges.

.. code-block:: shell

 $ kubectl apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml
 customresourcedefinition.apiextensions.k8s.io/rayclusters.cluster.ray.io created

Picking a Kubernetes namespace
-------------------------------
The rest of the Kubernetes resources we will use are namespaced. 
You can use an existing namespace for your Ray clusters or create a new one if you have permissions. 
For this example, we will create a namespace called ``ray``. 

.. code-block:: shell

 $ kubectl create namespace ray
 namespace/ray created

Starting the operator. 
----------------------

To launch the operator in our namespace, we execute the following command.

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml
 serviceaccount/ray-operator-serviceaccount created
 role.rbac.authorization.k8s.io/ray-operator-role created
 rolebinding.rbac.authorization.k8s.io/ray-operator-rolebinding created
 pod/ray-operator-pod created
 
The output shows that we've just launched a pod named ``ray-operator-pod`` -- this is the pod that runs the operator process.
The service account, role, and role binding we have created grant the pod the `permissions`_ it needs to manage Ray clusters. 

Launching Ray clusters
----------------------
Finally, to launch a Ray cluster, we create a RayCluster custom resource.

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml
 raycluster.cluster.ray.io/example-cluster created

The operator will detect the RayCluster resource we've created and will launch an autoscaling Ray cluster.
Our RayCluster configuration specifies ``minWorkers:2`` in the first entry of ``spec.podTypes``, so we get a head node and two workers upon launch.

.. note::

  To learn in more detail about RayCluster resources, we recommend take a looking at the annotated example ``example_cluster.yaml``  applied in the last command. 

.. code-block:: shell

 $ kubectl -n ray get pods
 NAME                               READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-hbxvv     1/1     Running   0          72s
 example-cluster-ray-worker-4hvv6   1/1     Running   0          64s
 example-cluster-ray-worker-78kp5   1/1     Running   0          64s
 ray-operator-pod                   1/1     Running   0          2m33s

We see four pods: the operator, the ray head node, and two ray worker nodes. 

Let's launch another cluster in the same namespace, this one specifiying ``minWorkers:1``.
.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

We confirm that both clusters are running in our namespace.

.. code-block:: shell

 $ kubectl -n ray get rayclusters
 NAME               AGE
 example-cluster    12m
 example-cluster2   114s

 $ kubectl -n ray get pods
 NAME                                READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-th4wv      1/1     Running   0          10m
 example-cluster-ray-worker-q9pjn    1/1     Running   0          10m
 example-cluster-ray-worker-qltnp    1/1     Running   0          10m
 example-cluster2-ray-head-kj5mg     1/1     Running   0          10s
 example-cluster2-ray-worker-qsgnd   1/1     Running   0          1s
 ray-operator-pod                    1/1     Running   0          10m

Now we can :ref:`run Ray programs<_ray_k8s-run>` on our Ray clusters.

Monitoring
----------
Autoscaling logs are written to the operator pod's stdout and can be accessed with :code:`kubectl logs`.
Each line of output is prefixed by the name of the cluster followed by colon.
The following command get the last hundred lines of autoscaling logs for our second cluster.  

.. code-block:: shell

 $ kubectl -n ray logs ray-operator-pod | grep ^example-cluster2: | tail -n 100

The output should include monitoring updates that look like this:

.. code-block:: shell

 example-cluster2:2020-12-11 12:01:06,021        DEBUG autoscaler.py:693 -- Cluster status: 1 nodes
 example-cluster2: - MostDelayedHeartbeats: {'172.17.0.7': 0.04234886169433594, '172.17.0.8': 0.042315006256103516}
 example-cluster2: - NodeIdleSeconds: Min=241 Mean=246 Max=251
 example-cluster2: - ResourceUsage: 0.0/2.0 CPU, 0.0 GiB/0.58 GiB memory, 0.0 GiB/0.1 GiB object_store_memory
 example-cluster2: - TimeSinceLastHeartbeat: Min=0 Mean=0 Max=0
 example-cluster2:Worker node types:
 example-cluster2: - worker-nodes: 1
 example-cluster2:2020-12-11 12:01:06,067        INFO resource_demand_scheduler.py:148 -- Cluster resources: [{'CPU': 1.0, 'node:172.17.0.7': 1.0, 'memory': 5.0, 'object_store_memory': 1.0}, {'object_store_memory': 1.0, 'memory': 6.0, 'CPU': 1.0, 'node:172.17.0.8': 1.0}]
 example-cluster2:2020-12-11 12:01:06,067        INFO resource_demand_scheduler.py:149 -- Node counts: defaultdict(<class 'int'>, {'head-node': 1, 'worker-nodes': 1})
 example-cluster2:2020-12-11 12:01:06,067        INFO resource_demand_scheduler.py:159 -- Placement group demands: []
 example-cluster2:2020-12-11 12:01:06,067        INFO resource_demand_scheduler.py:186 -- Resource demands: []
 example-cluster2:2020-12-11 12:01:06,067        INFO resource_demand_scheduler.py:187 -- Unfulfilled demands: []
 example-cluster2:2020-12-11 12:01:06,088        INFO resource_demand_scheduler.py:209 -- Node requests: {}
 example-cluster2:2020-12-11 12:01:06,098        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-qsgnd is not being updated and passes config check (can_update=True).
 example-cluster2:2020-12-11 12:01:06,111        DEBUG autoscaler.py:654 -- example-cluster2-ray-worker-qsgnd is not being updated and passes config check (can_update=True).


Updating and retrying
---------------------
To update a ray cluster's configuration, edit the configuration of the corresponding RayCluster resource
and apply it again:

.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster.yaml

To force a restart with the same configuration, you can add an `annotation`_ to the RayCluster resource's ``metadata.labels`` field, e.g.

.. code-block:: yaml
    
    apiVersion: cluster.ray.io/v1
    kind: RayCluster
    metadata:
      name: example-cluster
      annotations:
        try: again
    spec:
      ...

Then reapply the RayCluster, as above.

The same method can be used to restart the operator and all Ray clusters in the namespace: add an annotation to the pod named ``ray-operator-pod``
in the manifest `ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml` and reapply the manifest.

Currently, editing and reapplying a RayCluster resource will stop and restart Ray processes running on the corresponding
Ray cluster. Similarly, restarting the operator will stop and restart Ray processes on all Ray clusters in the operator's namespace.
This behavior may be modified in future releases.


Cleaning up
-----------
We shut down a Ray cluster by deleting the associated RayCluster resource.
Either of the next two commands will delete our second cluster ``example-cluster2``.

.. code-block:: shell

 $ kubectl -n ray delete raycluster example-cluster2
 # OR
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

The Pods associated with ``example-cluster2`` will go into ``TERMINATING`` status. In a few moments, we check that these pods are gone:

.. code-block:: shell

 $ kubectl -n ray get pods
 NAME                               READY   STATUS    RESTARTS   AGE
 example-cluster-ray-head-th4wv     1/1     Running   0          57m
 example-cluster-ray-worker-q9pjn   1/1     Running   0          56m
 example-cluster-ray-worker-qltnp   1/1     Running   0          56m
 ray-operator-pod                   1/1     Running   0          57m

Only the operator pod and the first ``example-cluster`` remain.

To finish clean-up, we delete our first cluster and then the operator's resources.

.. code-block:: shell

 $ kubectl -n ray delete raycluster test-cluster
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml

If you like, you can delete the RayCluster customer resource definition. 
(Using the operator again will then require reapplying the CRD.)

.. code-block:: shell

 $ kubectl delete crd rayclusters.cluster.ray.io
 # OR
 $ kubectl delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml

.. _`Kubernetes Operator`: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
.. _`Kubernetes Custom Resource`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _`Kubernetes Custom Resource Definition`: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
.. _`annotation`: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#attaching-metadata-to-objects
.. _`permissions`: https://kubernetes.io/docs/reference/access-authn-authz/rbac/
