.. _ref-autoscaler-sdk:

Autoscaler SDK
==============

.. _ref-autoscaler-sdk-request-resources:

ray.autoscaler.sdk.request.resources
------------------------------------

You can from within a Ray program command the autoscaler to scale the cluster up to a desired size with ``request_resources()`` call. The cluster will immediately attempt to scale to accomodate the requested resources, bypassing normal upscaling speed constraints.

.. autofunction:: ray.autoscaler.sdk.request_resources