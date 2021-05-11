Ray Operator Advanced Configuration
===================================

.. _k8s-advanced:

This document covers configuration options for the Ray Helm chart.
We recommend reading this :ref:`introductory guide<ray-k8s-deploy>` on the Helm chart first.


Running multiple Ray clusters
-----------------------------
The Ray Operator can manage multiple Ray clusters running within a single Kubernetes cluster.
Since Helm does not support sharing resources between different releases, an additional Ray cluster
must be launched in a Helm release from the separate from the release used to launch the Operator.

To enable a workflow with multiple Ray Clusters, the Ray Helm chart includes two flags:
- ``operatorOnly``: Start the Operator without launching a Ray cluster.
- ``clusterOnly``: Create a RayCluster custom resource without installing the Operator.
  (If the Operator has already been installed, a new Ray cluster will be launched.)

The following workflow will install an operator in 

.. code-block:: shell

   # Start the operator. Install a Ray cluster in a new namespace.
   helm -n ray install example-cluster --create-namespace ./ray

   # Start another Ray cluster.
   # The cluster will be managed by the operator created in the last command.
   helm -n ray install example-cluster2 --set clusterOnly=true

.. code-block:: shell

  # Install the operator in its own Helm release.
  helm install ray-operator --set operatorOnly=true ./ray

  # Install a Ray cluster in a new namespace.
  helm -n ray install example-cluster --set clusterOnly=true ./ray

  # Install another Ray cluster.
  helm -n ray install example-cluster2 --set clusterOnly=true

The Operator pod outputs autoscaling logs for all of the Ray clusters it manages.
Each line of output is prefixed by the string :code:`<cluster name>,<namespace>`.
This string can be used to filter for a specific Ray cluster's logs:

.. code-block:: shell

    # The last 100 lines of logging output for the cluster with name "example-cluster2" in namespace "ray":
    $ kubectl logs $(kubectl get pod -l cluster.ray.io/component=operator) | \
      grep example-cluster2,ray | tail -n 100


Cluster-scoped vs. namespaced operators
---------------------------------------
By default, Ray Helm chart installs a ``cluster-scoped`` operator.
This means that the operator manages all Ray clusters in your Kubernetes cluster, across all namespaces.
The namespace into which the Operator Deployment is launched is determined by the chart field ``operatorNamespace``.
If this field is unset, the operator is launched into namespace ``default``.

It is also possible to run a ``namespace-scoped`` Operator.
This means that the Operator is launched into the namespace of the Helm release and manages only
Ray clusters in that namespace. To run a namespaced Operator, add the flag ``--set namespacedOperator=True``
to your Helm install command.

.. warning::
   Do not simultaneously run namespaced and cluster-scoped Ray Operators within one Kubernetes cluster, as this will lead to unintended effects.

Helm chart configuration
------------------------
This section discusses the `RayCluster` configuration options exposed in the Ray Helm chart's `values.yaml`_ file.
A :ref:`Ray cluster<ray-cluster-overview>` consists of a head node and a collection of worker nodes.
When deploying Ray on Kubernetes, each Ray node runs in its own Kubernetes Pod.

The ``PodTypes`` field of ``values.yaml`` represents the pod configurations available for use as nodes in the RayCluster.
Each ``PodType`` has a ``name``, and ``headPodType`` identifies the name of the podType to use for the Ray head node.
The rest of the podTypes are used as configuration for the Ray worker nodes.

Each ``podType`` specifies ``minWorkers`` and ``maxWorkers`` fields.
The autoscaler will try to maintain at least ``minWorkers`` workers of the podType in the cluster and can scale up to
``maxWorkers``, according to the needs of Ray workload. A common pattern is to specify ``minWorkers`` = ``maxWorkers`` = 0
for the head ``podType`` to signal that the ``podType`` is to be used only for the head node.

The fields ``numCPU``, ``numGPU``, ``memory``, and ``nodeSelector`` determine the Kubernetes ``PodSpec`` to use for nodes
of the ``podType``. Refer to `values.yaml`_ for more details on these fields.





`minWorkers` specifies the minimum number of worker nodes, while `maxWorkers` constrains...
A common use case `minWorkers`... Another possibility is to set `minWorkers`.
These settings and others can be adjusted `helm upgrade`.

...

The rest of the fields correspond to configuration details.
For now -- ...
Full configurability -- ...
- The Ray maintainers greatly appreciate feedback..

Deploying without Helm
----------------------
It is possible to deploy the Ray Operator without Helm.
The necessary configuration files are available on the Ray GitHub under `deploy`_.
The following manifests must be installed in the order listed:

- The `RayCluster CRD`_
- The Ray Operator, `namespaced`_ or `cluster-scoped`_. Note that the cluster-scoped operator is configured to run in namespaced ``default``;
  modify as needed.
- A RayCluster custom resource, `example`_.

Cluster scoped operator
Namespaced operator
An example custom resource.
podTypes
take a look at.

Ray Cluster Lifecycle
---------------------

Restart behavior
~~~~~~~~~~~~~~~~
The Ray cluster will restart under the following circumstances:
  - There is an error in the cluster's autoscaling process. This will happen if the Ray head node goes down.
  - There has been a change to the Ray head pod configuration. In terms of the Ray Helm chart, this means that
    one of the following fields of the head's ``podType`` has been modified: ``numCPU``, ``numGPU``, ``memory``, ``nodeSelector``.

Similarly, all workers of a given ``podType`` will be discarded if
  - There has been a change to one of the following fields of the ``podType``: ``numCPU``, ``numGPU``, ``memory``, ``nodeSelector``.

Status information
~~~~~~~~~~~~~~~~~~

Running ``kubectl -n <namespace> get raycluster`` will show all Ray clusters in the namespace along with status info:

.. code-block:: shell

   kubectl -n ray get rayclusters

The ``STATUS`` column reports the RayCluster's ``status.phase`` field. The following values are possible:
  - Empty/nil: This means the RayCluster resource has not yet been registered by the Operator.
  - ``Updating``: The Operator is launching the Ray cluster or processing an update to the cluster's configuration.
  - ``Running``: The Ray cluster's autoscaling process is running in a normal state.
  - ``AutoscalingExceptionRecovery`` The Ray cluster's autoscaling process has crashed. Ray processes will restart. This can happen
    if the Ray head node goes down.
  - ``Error`` There was an unexpected error while updating the Ray cluster. The Ray maintainers would be grateful if you file an issue and include operator logs!

The ``RESTARTS`` column reports the RayCluster's ``status.autoscalerRetries`` field. This tracks the number of times the cluster has restarted due to an autoscaling error.


.. _`RayCluster CRD`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/crds/cluster_crd.yaml
.. _`namespaced`: https://github.com/ray-project/ray/tree/master/deploy/components/operator_namespaced.yaml
.. _`cluster-scoped`: https://github.com/ray-project/ray/tree/master/deploy/components/operator_cluster_scoped.yaml
.. _`example`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/
.. _`values.yaml`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/values.yaml

