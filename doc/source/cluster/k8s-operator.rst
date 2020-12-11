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

.. code-block:: shell

 $ kubectl -n ray get pods

We see four pods: the operator, the ray head node, and two ray worker nodes. 

Let's launch another cluster in the same namespace, this one specifiying ``minWorkers:1``.
.. code-block:: shell

 $ kubectl -n ray apply -f ray/python/ray/autoscaler/kubernetes/operator_configs/example_cluster2.yaml

We confirm that both clusters are running in our namespace.

.. code-block:: shell

 $ kubectl -n ray get rayclusters
 $ kubectl -n ray get pods

Now we can :ref:`run Ray programs<_ray_k8s-run>` on our Ray clusters.

Monitoring
----------
Autoscaling logs are written to the operator pod's stdout and can be accessed with :code:`kubectl logs`.
Each line of output is prefixed by the name of the cluster followed by colon.
The following command get the last fifty lines of autoscaling logs for our second cluster.  

.. code-block:: shell

 $ kubectl -n ray logs ray-operator-pod | grep ^example-cluster2: | tail -n 50

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

.. code-block:: shell

 $ kubectl -n ray delete raycluster test-cluster2

We check that the Pods associated with our second cluster are gone.

.. code-block:: shell

 $ kubectl -n ray get pods

To finish clean-up, we delete our first cluster and then the operator's resources.

.. code-block:: shell

 $ kubectl -n ray delete raycluster test-cluster
 $ kubectl -n ray delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/operator.yaml

If you like, you can delete the RayCluster customer resource definition. 
(Using the operator again will then require re-applying the CRD.)

.. code-block:: shell

 $ kubectl delete -f ray/python/ray/autoscaler/kubernetes/operator_configs/cluster_crd.yaml

.. _`Kubernetes Operator`: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
.. _`Kubernetes Custom Resource`: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
.. _`Kubernetes Custom Resource Definition`: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
.. _`annotation`: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/#attaching-metadata-to-objects
.. _`permissions`: https://kubernetes.io/docs/reference/access-authn-authz/rbac/

