.. include:: /_includes/clusters/we_are_hiring.rst

Monitoring and observability
----------------------------

Ray comes with following observability features:

1. :ref:`The dashboard <Ray-dashboard>`
2. :ref:`ray status <monitor-cluster>`
3. :ref:`Prometheus metrics <multi-node-metrics>`

Please refer to :ref:`the observability documentation <observability>` for more on Ray's observability features.


Monitoring the cluster via the dashboard
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`The dashboard provides detailed information about the state of the cluster <Ray-dashboard>`,
including the running jobs, actors, workers, nodes, etc.

By default, the cluster launcher and operator will launch the dashboard, but
not publicly expose it.

If you launch your application via the cluster launcher, you can securely
portforward local traffic to the dashboard via the ``ray dashboard`` command
(which establishes an SSH tunnel). The dashboard will now be visible at
``http://localhost:8265``.

The Kubernetes Operator makes the dashboard available via a Service targeting the Ray head pod.
You can :ref:`access the dashboard <ray-k8s-dashboard>` using ``kubectl port-forward``.


Observing the autoscaler
^^^^^^^^^^^^^^^^^^^^^^^^

The autoscaler makes decisions by scheduling information, and programmatic
information from the cluster. This information, along with the status of
starting nodes, can be accessed via the ``ray status`` command.

To dump the current state of a cluster launched via the cluster launcher, you
can run ``ray exec cluster.yaml "Ray status"``.

For a more "live" monitoring experience, it is recommended that you run ``ray
status`` in a watch loop: ``ray exec cluster.yaml "watch -n 1 Ray status"``.

With the kubernetes operator, you should replace ``ray exec cluster.yaml`` with
``kubectl exec <head node pod>``.

Prometheus metrics
^^^^^^^^^^^^^^^^^^

Ray is capable of producing prometheus metrics. When enabled, Ray produces some
metrics about the Ray core, and some internal metrics by default. It also
supports custom, user-defined metrics.

These metrics can be consumed by any metrics infrastructure which can ingest
metrics from the prometheus server on the head node of the cluster.

:ref:`Learn more about setting up prometheus here. <multi-node-metrics>`
