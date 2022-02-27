:orphan:

.. include:: we_are_hiring.rst

.. _k8s-advanced:

Ray Operator Advanced Configuration
===================================
This document covers configuration options and other details concerning autoscaling Ray clusters on Kubernetes.
We recommend first reading this :ref:`introductory guide<ray-k8s-deploy>`.

.. _helm-config:

Helm chart configuration
------------------------
This section discusses ``RayCluster`` configuration options exposed in the Ray Helm chart's `values.yaml`_ file.
The default settings in ``values.yaml`` were chosen for the purposes of demonstration.
For production use cases, the values should be modified. For example, you will probably want to increase Ray Pod resource requests.

Setting custom chart values
~~~~~~~~~~~~~~~~~~~~~~~~~~~
To configure Helm chart values, you can pass in a custom values ``yaml`` and/or set individual fields.

.. code-block:: shell

   # Pass in a custom values yaml.
   $ helm install example-cluster -f custom_values.yaml ./ray
   # Set custom values on the command line.
   $ helm install example-cluster --set image=rayproject/ray:1.2.0 ./ray

Refer to the `Helm docs`_ for more information.

Ray cluster configuration
~~~~~~~~~~~~~~~~~~~~~~~~~
A :ref:`Ray cluster<cluster-index>` consists of a head node and a collection of worker nodes.
When deploying Ray on Kubernetes, each Ray node runs in its own Kubernetes Pod.
The ``podTypes`` field of ``values.yaml`` represents the pod configurations available for use as nodes in the Ray cluster.
The key of each ``podType`` is a user-defined name. The field ``headPodType`` identifies the name of the ``podType`` to use for the Ray head node.
The rest of the ``podTypes`` are used as configuration for the Ray worker nodes.

Each ``podType`` specifies ``minWorkers`` and ``maxWorkers`` fields.
The autoscaler will try to maintain at least ``minWorkers`` of the ``podType`` and can scale up to
``maxWorkers`` according to the resource demands of the Ray workload. A common pattern is to specify ``minWorkers`` = ``maxWorkers`` = 0
for the head ``podType``; this signals that the ``podType`` is to be used only for the head node.
You can use `helm upgrade`_ to adjust the fields ``minWorkers`` and ``maxWorkers`` without :ref:`restarting<k8s-restarts>` the Ray cluster.

The fields ``CPU``, ``GPU``, ``memory``, and ``nodeSelector`` configure the Kubernetes ``PodSpec`` to use for nodes
of the ``podType``. The ``image`` field determines the Ray container image used by all nodes in the Ray cluster.

The ``rayResources`` field of each ``podType`` can be used to signal the presence of custom resources to Ray.
To schedule Ray tasks and actors that use custom hardware resources, ``rayResources`` can be used in conjunction with
``nodeSelector``:

- Use ``nodeSelector`` to constrain workers of a ``podType`` to run on a Kubernetes Node with specialized hardware (e.g. a particular GPU accelerator.)
- Signal availability of the hardware for that ``podType`` with ``rayResources: {"custom_resource": 3}``.
- Schedule a Ray task or actor to use that resource with ``@ray.remote(resources={"custom_resource": 1})``.

By default, the fields ``CPU``, ``GPU``, and ``memory`` are used to configure cpu, gpu, and memory resources advertised to Ray.
However, ``rayResources`` can be used to override this behavior. For example, ``rayResources: {"CPU": 0}`` can be set for head podType,
to :ref:``avoid scheduling tasks on the Ray head``.

Refer to the documentation in `values.yaml`_ for more details.

.. note::

  If your application could benefit from additional configuration options in the Ray Helm chart,
  (e.g. exposing more PodSpec fields), feel free to open a `feature request`_ on
  the Ray GitHub or a `discussion thread`_ on the Ray forums.

  For complete configurability, it is also possible launch a Ray cluster :ref:`without the Helm chart<no-helm>`
  or to modify the Helm chart.

.. note::

  Some things to keep in mind about the scheduling of Ray worker pods and Ray tasks/actors:

  1. The Ray Autoscaler executes scaling decisions by sending pod creation requests to the Kubernetes API server.
  If your Kubernetes cluster cannot accomodate more worker pods of a given ``podType``, requested pods will enter
  a ``Pending`` state until the pod can be scheduled or a `timeout`_ expires.

  2. If a Ray task requests more resources than available in any ``podType``, the Ray task cannot be scheduled.


Running multiple Ray clusters
-----------------------------
The Ray Operator can manage multiple Ray clusters running within a single Kubernetes cluster.
Since Helm does not support sharing resources between different releases, an additional Ray cluster
must be launched in a Helm release separate from the release used to launch the Operator.

To enable launching with multiple Ray Clusters, the Ray Helm chart includes two flags:

- ``operatorOnly``: Start the Operator without launching a Ray cluster.
- ``clusterOnly``: Create a RayCluster custom resource without installing the Operator. \(If the Operator has already been installed, a new Ray cluster will be launched.)

The following commands will install the Operator and two Ray Clusters in
three separate Helm releases:

.. code-block:: shell

  # Install the operator in its own Helm release.
  $ helm install ray-operator --set operatorOnly=true ./ray

  # Install a Ray cluster in a new namespace "ray".
  $ helm -n ray install example-cluster --set clusterOnly=true ./ray --create-namespace

  # Install a second Ray cluster. Launch the second cluster without any workers.
  $ helm -n ray install example-cluster2 \
      --set podTypes.rayWorkerType.minWorkers=0 --set clusterOnly=true ./ray

  # Examine the pods in both clusters.
  $ kubectl -n ray get pods
  NAME                                    READY   STATUS    RESTARTS   AGE
   example-cluster-ray-head-type-v6tt9     1/1     Running   0          35s
   example-cluster-ray-worker-type-fmn4k   1/1     Running   0          22s
   example-cluster-ray-worker-type-r6m7k   1/1     Running   0          22s
   example-cluster2-ray-head-type-tj666    1/1     Running   0          15s

Alternatively, the Operator and one of the Ray Clusters can be installed in the same Helm release:

.. code-block:: shell

   # Start the operator. Install a Ray cluster in a new namespace.
   helm -n ray install example-cluster --create-namespace ./ray

   # Start another Ray cluster.
   # The cluster will be managed by the operator created in the last command.
   $ helm -n ray install example-cluster2 \
      --set podTypes.rayWorkerType.minWorkers=0 --set clusterOnly=true ./ray


The Operator pod outputs autoscaling logs for all of the Ray clusters it manages.
Each line of output is prefixed by the string :code:`<cluster name>,<namespace>`.
This string can be used to filter for a specific Ray cluster's logs:

.. code-block:: shell

    # The last 100 lines of logging output for the cluster with name "example-cluster2" in namespace "ray":
    $ kubectl logs \
      $(kubectl get pod -l cluster.ray.io/component=operator -o custom-columns=:metadata.name) \
      | grep example-cluster2,ray | tail -n 100

.. _k8s-cleanup:

Cleaning up resources
---------------------
When cleaning up,
**RayCluster resources must be deleted before the Operator deployment is deleted**.
This is because the Operator must remove a `finalizer`_ from the ``RayCluster`` resource to allow
deletion of the resource to complete.

If the Operator and ``RayCluster`` are created as part of the same Helm release,
the ``RayCluster`` must be deleted :ref:`before<k8s-cleanup-basic>` uninstalling the Helm release.
If the Operator and one or more ``RayClusters`` are created in multiple Helm releases,
the ``RayCluster`` releases must be uninstalled before the Operator release.

To remedy a situation where the Operator deployment was deleted first and ``RayCluster`` deletion is hanging, try one of the following:

- Manually delete the ``RayCluster``'s finalizers with ``kubectl edit`` or ``kubectl patch``.
- Restart the Operator so that it can remove ``RayCluster`` finalizers. Then remove the Operator.

Cluster-scoped vs. namespaced operators
---------------------------------------
By default, the Ray Helm chart installs a ``cluster-scoped`` operator.
This means that the operator manages all Ray clusters in your Kubernetes cluster, across all namespaces.
The namespace into which the Operator Deployment is launched is determined by the chart field ``operatorNamespace``.
If this field is unset, the operator is launched into namespace ``default``.

It is also possible to run a ``namespace-scoped`` Operator.
This means that the Operator is launched into the namespace of the Helm release and manages only
Ray clusters in that namespace. To run a namespaced Operator, add the flag ``--set namespacedOperator=True``
to your Helm install command.

.. warning::
   Do not simultaneously run namespaced and cluster-scoped Ray Operators within one Kubernetes cluster, as this will lead to unintended effects.

.. _no-helm:

Deploying without Helm
----------------------
It is possible to deploy the Ray Operator without Helm.
The necessary configuration files are available on the `Ray GitHub`_.
The following manifests should be installed in the order listed:

- The `RayCluster CRD`_.
- The Ray Operator, `namespaced`_ or `cluster-scoped`_.\Note that the cluster-scoped operator is configured to run in namespaced ``default``. Modify as needed.
- A RayCluster custom resource: `example`_.

Ray Cluster Lifecycle
---------------------

.. _k8s-restarts:

Restart behavior
~~~~~~~~~~~~~~~~

The Ray cluster will restart under the following circumstances:

- There is an error in the cluster's autoscaling process. This will happen if the Ray head node goes down.
- There has been a change to the Ray head pod configuration. In terms of the Ray Helm chart, this means either ``image`` or one of the following fields of the head's ``podType`` has been modified: ``CPU``, ``GPU``, ``memory``, ``nodeSelector``.

Similarly, all workers of a given ``podType`` will be discarded if

- There has been a change to ``image`` or one of the following fields of the ``podType``: ``CPU``, ``GPU``, ``memory``, ``nodeSelector``.

Status information
~~~~~~~~~~~~~~~~~~

Running ``kubectl -n <namespace> get raycluster`` will show all Ray clusters in the namespace with status information.

.. code-block:: shell

   kubectl -n ray get rayclusters
   NAME              STATUS    RESTARTS   AGE
   example-cluster   Running   0          9s

The ``STATUS`` column reports the RayCluster's ``status.phase`` field. The following values are possible:

- ``Empty/nil``: This means the RayCluster resource has not yet been registered by the Operator.
- ``Updating``: The Operator is launching the Ray cluster or processing an update to the cluster's configuration.
- ``Running``: The Ray cluster's autoscaling process is running in a normal state.
- ``AutoscalingExceptionRecovery`` The Ray cluster's autoscaling process has crashed. Ray processes will restart. This can happen if the Ray head node goes down.
- ``Error`` There was an unexpected error while updating the Ray cluster. (The Ray maintainers would be grateful if you file a `bug report`_ with operator logs.)

The ``RESTARTS`` column reports the RayCluster's ``status.autoscalerRetries`` field. This tracks the number of times the cluster has restarted due to an autoscaling error.

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst

.. _`RayCluster CRD`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/crds/cluster_crd.yaml
.. _`finalizer` : https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#finalizers
.. _`namespaced`: https://github.com/ray-project/ray/tree/master/deploy/components/operator_namespaced.yaml
.. _`cluster-scoped`: https://github.com/ray-project/ray/tree/master/deploy/components/operator_cluster_scoped.yaml
.. _`example`: https://github.com/ray-project/ray/tree/master/deploy/components/example_cluster.yaml
.. _`values.yaml`: https://github.com/ray-project/ray/tree/master/deploy/charts/ray/values.yaml
.. _`bug report`: https://github.com/ray-project/ray/issues/new?assignees=&labels=bug%2C+triage&template=bug_report.md&title=
.. _`helm upgrade`: https://helm.sh/docs/helm/helm_upgrade/
.. _`feature request`: https://github.com/ray-project/ray/issues/new?assignees=&labels=enhancement&template=feature_request.md&title=
.. _`discussion thread`: https://discuss.ray.io/c/ray-clusters/ray-kubernetes/11
.. _`timeout`: https://github.com/ray-project/ray/blob/b08b2c5103c634c680de31b237b2bfcceb9bc150/python/ray/autoscaler/_private/constants.py#L22
.. _`Helm docs`: https://helm.sh/docs/helm/helm_install/
.. _`Ray GitHub`: https://github.com/ray-project/ray/tree/master/deploy/components/
