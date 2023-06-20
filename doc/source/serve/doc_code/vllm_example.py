import json
import random
from typing import AsyncGenerator
from starlette.requests import Request
from starlette.responses import StreamingResponse, Response

from transformers import AutoTokenizer

from ray import serve

from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.engine.async_llm_engine import AsyncLLMEngine
from vllm.sampling_params import SamplingParams
from vllm.utils import random_uuid


@serve.deployment(ray_actor_options={"num_gpus": 1})
class VLLMPredictDeployment:
    def __init__(self, **kwargs):
        args = AsyncEngineArgs(**kwargs)
        self.engine = AsyncLLMEngine.from_engine_args(args)
        
    async def __call__(self, request: Request) -> Response:
        """Generate completion for the request.

        The request should be a JSON object with the following fields:
        - prompt: the prompt to use for the generation.
        - stream: whether to stream the results or not.
        - other fields: the sampling parameters (See `SamplingParams` for details).
        """
        request_dict = await request.json()
        prompt = request_dict.pop("prompt")
        stream = request_dict.pop("stream", False)
        sampling_params = SamplingParams(**request_dict)
        request_id = random_uuid()
        results_generator = self.engine.generate(prompt, sampling_params, request_id)

        # Streaming case
        async def stream_results() -> AsyncGenerator[bytes, None]:
            async for request_output in results_generator:
                prompt = request_output.prompt
                text_outputs = [
                    prompt + output.text
                    for output in request_output.outputs
                ]
                ret = {"text": text_outputs}
                yield (json.dumps(ret) + "\0").encode("utf-8")

        async def abort_request() -> None:
            await engine.abort(request_id)

        if stream:
            background_tasks = BackgroundTasks()
            # Abort the request if the client disconnects.
            background_tasks.add_task(abort_request)
            return StreamingResponse(stream_results(), background=background_tasks)

        # Non-streaming case
        final_output = None
        async for request_output in results_generator:
            if await request.is_disconnected():
                # Abort the request if the client disconnects.
                await engine.abort(request_id)
                return Response(status_code=499)
            final_output = request_output

        assert final_output is not None
        prompt = final_output.prompt
        text_outputs = [
            prompt + output.text
            for output in final_output.outputs
        ]
        ret = {"text": text_outputs}
        return Response(content=json.dumps(ret))


def send_request():
    import requests
    prompt = "how to cook fried rice?"
    sample_input = {"prompt": prompt}
    output = requests.post("http://localhost:8000/", json=sample_input)
    print(output.json())


if __name__ == "__main__":
    deployment = VLLMPredictDeployment.bind(model="facebook/opt-125m")
    serve.run(deployment)
    send_request()