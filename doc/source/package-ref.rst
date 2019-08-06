Ray Package Reference
=====================

.. autofunction:: ray.init

.. autofunction:: ray.is_initialized

.. autofunction:: ray.remote

.. autofunction:: ray.get

.. autofunction:: ray.wait

.. autofunction:: ray.put

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

.. autofunction:: ray.tasks

.. autofunction:: ray.objects

.. autofunction:: ray.timeline

.. autofunction:: ray.object_transfer_timeline

.. autofunction:: ray.cluster_resources

.. autofunction:: ray.available_resources

.. autofunction:: ray.errors


The Ray Command Line API
------------------------

.. click:: ray.scripts.scripts:start
   :prog: ray start
   :show-nested:

.. click:: ray.scripts.scripts:stop
   :prog: ray stop
   :show-nested:

.. click:: ray.scripts.scripts:create_or_update
   :prog: ray up
   :show-nested:

.. click:: ray.scripts.scripts:teardown
   :prog: ray down
   :show-nested:

.. click:: ray.scripts.scripts:exec_cmd
   :prog: ray exec
   :show-nested:

.. click:: ray.scripts.scripts:attach
   :prog: ray attach
   :show-nested:

.. click:: ray.scripts.scripts:get_head_ip
   :prog: ray get_head_ip
   :show-nested:

.. click:: ray.scripts.scripts:stack
   :prog: ray stack
   :show-nested:

.. click:: ray.scripts.scripts:timeline
   :prog: ray timeline
   :show-nested:
