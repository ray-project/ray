# flake8: noqa
# __response_by_reference_example_start__
# File name: response_to_object_ref.py
import asyncio
from typing import List

import ray
from ray import ObjectRef

from ray import serve
from ray.serve.handle import DeploymentHandle, DeploymentResponse


@serve.deployment
class Preprocessor:
    def __call__(self) -> str:
        return "Hello from preprocessor!"


@serve.deployment
class DownstreamModel:
    def pass_by_value(self, result: str) -> str:
        return f"Got result by value: '{result}'"

    async def pass_by_reference(self, wrapped_result: List[ObjectRef]) -> str:
        # Do some preprocessing work before fetching the result.
        await asyncio.sleep(0.1)

        result: str = await wrapped_result[0]
        return f"Got result by reference: '{result}'"


@serve.deployment
class Ingress:
    def __init__(self, preprocessor: DeploymentHandle, downstream: DeploymentHandle):
        self._preprocessor = preprocessor
        self._downstream = downstream

    async def __call__(self, mode: str) -> str:
        preprocessor_response: DeploymentResponse = self._preprocessor.remote()
        if mode == "by_value":
            return await self._downstream.pass_by_value.remote(preprocessor_response)
        else:
            return await self._downstream.pass_by_reference.remote(
                [preprocessor_response]
            )


app = Ingress.bind(Preprocessor.bind(), DownstreamModel.bind())
handle: DeploymentHandle = serve.run(app)

by_value_result = handle.remote(mode="by_value").result()
assert by_value_result == "Got result by value: 'Hello from preprocessor!'"

by_reference_result = handle.remote(mode="by_reference").result()
assert by_reference_result == "Got result by reference: 'Hello from preprocessor!'"
# __response_by_reference_example_end__
