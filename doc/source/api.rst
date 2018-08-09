The Ray API
===========

.. autofunction:: ray.init

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
