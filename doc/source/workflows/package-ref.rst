Ray Workflows API
=================

Core API
---------
.. autofunction:: ray.workflow.init
.. autofunction:: ray.workflow.step
.. autoclass:: ray.workflow.common.Workflow
    :members:

Virtual Actors
--------------
.. autofunction:: ray.workflow.virtual_actor
.. autoclass:: ray.workflow.virtual_actor_class.VirtualActorClass
    :members:

Management API
--------------
.. autofunction:: ray.workflow.resume_all
.. autofunction:: ray.workflow.resume
.. autofunction:: ray.workflow.list_all
.. autofunction:: ray.workflow.get_status
.. autofunction:: ray.workflow.get_output
.. autofunction:: ray.workflow.get_metadata
.. autofunction:: ray.workflow.get_actor
.. autofunction:: ray.workflow.cancel
