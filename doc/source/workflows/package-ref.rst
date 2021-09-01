Workflows API Reference
=======================

Core API
---------
.. autofunction:: ray.experimental.workflow.init
.. autofunction:: ray.experimental.workflow.step
.. autoclass:: ray.experimental.workflow.common.Workflow
    :members:

Virtual Actors
--------------
.. autofunction:: ray.experimental.workflow.virtual_actor
.. autoclass:: ray.experimental.workflow.virtual_actor_class.VirtualActorClass
    :members:

Management API
--------------
.. autofunction:: ray.experimental.workflow.resume_all
.. autofunction:: ray.experimental.workflow.resume
.. autofunction:: ray.experimental.workflow.list_all
.. autofunction:: ray.experimental.workflow.get_status
.. autofunction:: ray.experimental.workflow.get_output
.. autofunction:: ray.experimental.workflow.get_actor
.. autofunction:: ray.experimental.workflow.cancel
