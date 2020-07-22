API and Package Reference
=========================

Python API
----------

.. _ray-init-ref:

ray.init
~~~~~~~~

.. autofunction:: ray.init

.. _ray-is_initialized-ref:

ray.is_initialized
~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.is_initialized

.. _ray-remote-ref:

ray.remote
~~~~~~~~~~

.. autofunction:: ray.remote

.. _ray-get-ref:

ray.get
~~~~~~~

.. autofunction:: ray.get

.. _ray-wait-ref:

ray.wait
~~~~~~~~

.. autofunction:: ray.wait

.. _ray-put-ref:

ray.put
~~~~~~~

.. autofunction:: ray.put

.. _ray-kill-ref:

ray.kill
~~~~~~~~

.. autofunction:: ray.kill

.. _ray-cancel-ref:

ray.cancel
~~~~~~~~~~

.. autofunction:: ray.cancel

.. _ray-get_gpu_ids-ref:

ray.get_gpu_ids
~~~~~~~~~~~~~~~

.. autofunction:: ray.get_gpu_ids

.. _ray-get_resource_ids-ref:

ray.get_resource_ids
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.get_resource_ids

.. _ray-get_webui_url-ref:

ray.get_webui_url
~~~~~~~~~~~~~~~~~

.. autofunction:: ray.get_webui_url

.. _ray-shutdown-ref:

ray.shutdown
~~~~~~~~~~~~

.. autofunction:: ray.shutdown


.. _ray-register_custom_serializer-ref:

ray.register_custom_serializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.register_custom_serializer

.. _ray-profile-ref:

ray.profile
~~~~~~~~~~~

.. autofunction:: ray.profile

.. _ray-method-ref:

ray.method
~~~~~~~~~~

.. autofunction:: ray.method

ray.util.ActorPool
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.ActorPool
   :members:

.. _ray-nodes-ref:

ray.nodes
~~~~~~~~~

.. autofunction:: ray.nodes

.. _ray-objects-ref:

ray.objects
~~~~~~~~~~~

.. autofunction:: ray.objects

.. _ray-timeline-ref:

ray.timeline
~~~~~~~~~~~~

.. autofunction:: ray.timeline

.. _ray-object_transfer_timeline-ref:

ray.object_transfer_timeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.object_transfer_timeline

.. _ray-cluster_resources-ref:

ray.cluster_resources
~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.cluster_resources

.. _ray-available_resources-ref:

ray.available_resources
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.available_resources

.. _ray-errors-ref:

ray.errors
~~~~~~~~~~

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
