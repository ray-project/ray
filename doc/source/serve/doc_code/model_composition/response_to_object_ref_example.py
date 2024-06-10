# flake8: noqa
# __response_to_object_ref_example_start__
# File name: response_to_object_ref.py
import ray
from ray import serve
from ray.serve.handle import DeploymentHandle, DeploymentResponse


@ray.remote
def say_hi_task(inp: str):
    return f"Ray task got message: '{inp}'"


@serve.deployment
class SayHi:
    def __call__(self) -> str:
        return "Hi from Serve deployment"


@serve.deployment
class Ingress:
    def __init__(self, say_hi: DeploymentHandle):
        self._say_hi = say_hi

    async def __call__(self):
        # Make a call to the SayHi deployment and pass the result ref to
        # a downstream Ray task.
        response: DeploymentResponse = self._say_hi.remote()
        response_obj_ref: ray.ObjectRef = await response._to_object_ref()
        final_obj_ref: ray.ObjectRef = say_hi_task.remote(response_obj_ref)
        return await final_obj_ref


app = Ingress.bind(SayHi.bind())
handle: DeploymentHandle = serve.run(app)
assert handle.remote().result() == "Ray task got message: 'Hi from Serve deployment'"
# __response_to_object_ref_example_end__
