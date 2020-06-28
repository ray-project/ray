Ray Package Reference
=====================

.. autofunction:: ray.init

.. autofunction:: ray.is_initialized

.. autofunction:: ray.remote

.. autofunction:: ray.get

.. autofunction:: ray.wait

.. autofunction:: ray.put

.. autofunction:: ray.kill

.. autofunction:: ray.cancel

.. autofunction:: ray.get_gpu_ids

.. autofunction:: ray.get_resource_ids

.. autofunction:: ray.get_webui_url

.. autofunction:: ray.shutdown

.. autofunction:: ray.register_custom_serializer

.. autofunction:: ray.profile

.. autofunction:: ray.method

Inspect the Cluster State
-------------------------

.. autofunction:: ray.nodes

.. autofunction:: ray.objects

.. autofunction:: ray.timeline

.. autofunction:: ray.object_transfer_timeline

.. autofunction:: ray.cluster_resources

.. autofunction:: ray.available_resources

.. autofunction:: ray.errors

Experimental APIs
-----------------

.. automodule:: ray.experimental
   :members:

The Ray Command Line API
------------------------

.. _ray-start-doc:

.. click:: ray.scripts.scripts:start
   :prog: ray start
   :show-nested:

.. _ray-stop-doc:

.. click:: ray.scripts.scripts:stop
   :prog: ray stop
   :show-nested:

.. _ray-up-doc:

.. click:: ray.scripts.scripts:up
   :prog: ray up
   :show-nested:

.. _ray-down-doc:

.. click:: ray.scripts.scripts:down
   :prog: ray down
   :show-nested:

.. _ray-exec-doc:

.. click:: ray.scripts.scripts:exec
   :prog: ray exec
   :show-nested:

.. _ray-submit-doc:

.. click:: ray.scripts.scripts:submit
   :prog: ray submit
   :show-nested:

.. _ray-attach-doc:

.. click:: ray.scripts.scripts:attach
   :prog: ray attach
   :show-nested:

.. _ray-get_head_ip-doc:

.. click:: ray.scripts.scripts:get_head_ip
   :prog: ray get_head_ip
   :show-nested:

.. _ray-stack-doc:

.. click:: ray.scripts.scripts:stack
   :prog: ray stack
   :show-nested:

.. _ray-stat-doc:

.. click:: ray.scripts.scripts:statistics
   :prog: ray statistics
   :show-nested:

.. _ray-memory-doc:

.. click:: ray.scripts.scripts:memory
   :prog: ray memory
   :show-nested:

.. _ray-globalgc-doc:

.. click:: ray.scripts.scripts:globalgc
   :prog: ray globalgc
   :show-nested:

.. _ray-timeline-doc:

.. click:: ray.scripts.scripts:timeline
   :prog: ray timeline
   :show-nested:
