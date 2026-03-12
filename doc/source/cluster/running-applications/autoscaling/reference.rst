.. _ref-autoscaler-sdk:

Programmatic Cluster Scaling
============================

.. _ref-autoscaler-sdk-request-resources:

ray.autoscaler.sdk.request_resources
------------------------------------

Within a Ray program, you can command the autoscaler to scale the cluster up to a desired size with ``request_resources()`` call. The cluster will immediately attempt to scale to accommodate the requested resources, bypassing normal upscaling speed constraints.

.. note::

    ``bundle_label_selectors`` are enforced only by autoscaler v2.
    If you switch from autoscaler v2 to v1, label selector constraints are
    ignored and only resource quantities in bundles are considered.
    
.. autofunction:: ray.autoscaler.sdk.request_resources
    :noindex:
