Ray Operator Advanced Configuration
===================================

Running multiple Ray clusters
-----------------------------
The Ray Operator can manage multiple Ray Clusters running within a single Kubernetes cluster.
However, Helm does not support sharing resources between different releases.
Scenario -- as part of common setup. Then install using...
As a solution to this, the Ray Helm chart supports
- ``operatorOnly`` Starting the Operator without
- ``clusterOnly`` Create a RayCluster custom resource without installing the Operator.
  (If the Operator has already been installed, a new Ray Cluster will be launched.)

Either of the following workflows may be used to install two Ray clusters in a namespace called ``ray``:
``
# Install the operator without starting a Ray Cluster.
# Install a Ray Cluster in a new namespace.
# Install a second Ray Cluster in the same namespace.
...
# Clean up.
# (Optionally, delete the CRD)
``
or

``
# Install the operator and a Ray Cluster.
# Install a second Ray Cluster.
...
# Clean up.
# (Optional)
``

``
kubectl get pods

kubectl logs ray-operator | grep example-cluster,ray
kubectl logs ray-operator | grep example-cluster2,ray
``

Cluster-scoped vs. namespaced operators
---------------------------------------
By default cluster-scoped operator.
This means that the operator manages all Ray clusters in your Kubernetes cluster.
The namespace the ``operatorNamespace``. If the option is unset, in namespace ``default``.

It is also possible to run a namespace-scoped.
This means that the Operator manages only the Ray clusters in the namespace of the Helm release.
Choose the option that...

Helm chart configuration
------------------------
Details.
`minWorkers` specifies the minimum number of worker nodes, while `maxWorkers` constrains...
A common use case `minWorkers`... Another possibility is to set `minWorkers`.
These settings and others can be adjusted `helm upgrade`.

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
