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
Details.
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
Cluster scoped operator
Namespaced operator
An example custom resource.
podTypes
take a look at.

Ray cluster lifecycle
---------------------
