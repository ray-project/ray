===
FAQ
===

These are some Frequently Asked Questions that we've seen pop up for using Ray clusters.
If you still have questions after reading this FAQ,  please reach out on
`our Discourse <https://discuss.ray.io/>`__!

Do Ray clusters support multi-tenancy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, you can run multiple :ref:`jobs <jobs-overview>` from different users simultaneously in a Ray cluster
but it's NOT recommended in production.
The reason is that Ray currently still misses some features for multi-tenancy in production:

* Ray doesn't provide strong resource isolation:
  Ray :ref:`resources <core-resources>` are logical and they don't limit the physical resources a task or actor can use while running.
  This means simultaneous jobs can interfere with each other and makes them less reliable to run in production.

* Ray doesn't support priorities: All jobs, tasks and actors have the same priority so there is no way to prioritize important jobs under load.

* Ray doesn't support access control: jobs have full access to a Ray cluster and all of the resources within it.

On the other hand, you can run the same job multiple times using the same cluster to save the cluster startup time.

.. note::
    A Ray :ref:`namespace <namespaces-guide>` is just a logical grouping of jobs and named actors. Unlike a Kubernetes namespace, it doesn't provide any other multi-tenancy functions like resource quotas.


I have multiple Ray users. What's the right way to deploy Ray for them?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's recommended to start a Ray cluster for each user so that their workloads are isolated.
