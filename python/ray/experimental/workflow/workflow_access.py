from typing import Any

import ray


@ray.remote
def workflow_output_cache(manager_actor: ray.actor.ActorHandle,
                          workflow_id: str, output: ray.ObjectRef) -> Any:
    while isinstance(output, ray.ObjectRef):
        output = ray.get(output)
    manager_actor.notify_complete(workflow_id)
    return output


@ray.remote
class WorkflowManagement:
    def __init__(self):
        pass

    def notify_complete(self, workflow_id: str):
        pass
