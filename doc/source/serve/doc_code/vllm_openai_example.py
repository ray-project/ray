# __serve_example_begin__
from typing import Dict, Optional, List
import logging

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import StreamingResponse, JSONResponse

from ray import serve

from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.engine.async_llm_engine import AsyncLLMEngine
from vllm.entrypoints.openai.cli_args import make_arg_parser
from vllm.entrypoints.openai.protocol import (ChatCompletionRequest,
                                            ChatCompletionResponse,
                                            ErrorResponse)
from vllm.entrypoints.openai.serving_chat import OpenAIServingChat
from vllm.entrypoints.openai.serving_engine import LoRAModulePath

logger = logging.getLogger("ray.serve")

app = FastAPI()

@serve.deployment(autoscaling_config={"min_replicas": 1, "max_replicas": 10, "target_ongoing_requests": 5},
                max_ongoing_requests=10)
@serve.ingress(app)
class VLLMDeployment:

    def __init__(self,
            engine_args: AsyncEngineArgs,
            response_role: str,
            lora_modules: Optional[List[LoRAModulePath]]=None,
            chat_template: Optional[str] = None):

        logger.info(f"Starting with engine args: {engine_args}")
        self.engine = AsyncLLMEngine.from_engine_args(engine_args)

        # determine the name of the served model for OpenAI client
        if engine_args.served_model_name is not None:
            served_model_names = engine_args.served_model_name
        else:
            served_model_names = [engine_args.model]
        self.openai_serving_chat = OpenAIServingChat(self.engine,
                                                        served_model_names,
                                                        response_role,
                                                        lora_modules,
                                                        chat_template)

    # API Reference here - https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html
    @app.post("/v1/chat/completions")
    async def create_chat_completion(self,
                                request: ChatCompletionRequest,
                                raw_request: Request):
        logger.info("request", request)
        logger.info("raw request", raw_request)
        generator = await self.openai_serving_chat.create_chat_completion(
            request, raw_request)
        if isinstance(generator, ErrorResponse):
            return JSONResponse(content=generator.model_dump(),
                                status_code=generator.code)
        if request.stream:
            return StreamingResponse(content=generator,
                                    media_type="text/event-stream")
        else:
            assert isinstance(generator, ChatCompletionResponse)
            return JSONResponse(content=generator.model_dump())

'''
This method parses the vLLM args using the vLLM parser based on CLI inputs. 
'''
def parse_vllm_args(cli_args: Dict[str, str]):
    parser = make_arg_parser()
    arg_strings = []
    for key, value in cli_args.items():
        arg_strings.extend([f'--{key}', str(value)])
    logger.info(arg_strings)
    parsed_args = parser.parse_args(args=arg_strings)
    return parsed_args

def build_app(cli_args: Dict[str, str]) -> serve.Application:
    """
    Refer to https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#command-line-arguments-for-the-server for complete set of arguments
    Supported engine arguments are here - https://docs.vllm.ai/en/latest/models/engine_args.html
    """
    parsed_args = parse_vllm_args(cli_args)
    engine_args = AsyncEngineArgs.from_cli_args(parsed_args)
    engine_args.worker_use_ray = True
    tp = engine_args.tensor_parallel_size
    pg_resources = []
    logger.info(f"Tensor parallelism = {tp}")
    pg_resources.append({"CPU": 1}) # for the deployment
    for i in range(tp):
        pg_resources.append({"CPU": 1, "GPU": 1}) # for the vLLM Ray Actors
    # We use the "STRICT_PACK" strategy below to ensure all vLLM Actors are on the same ray node
    return VLLMDeployment.options(placement_group_bundles=pg_resources, 
                placement_group_strategy="STRICT_PACK").bind(engine_args,
                parsed_args.response_role, parsed_args.lora_modules,
                parsed_args.chat_template)


# __serve_example_end__

if __name__ == "__main__":
    serve.run(build_app(cli_args = {"model": "NousResearch/Meta-Llama-3-8B-Instruct",
                                "tensor-parallel-size": "1"}))
    # __query_example_begin__
    from openai import OpenAI
    # Note: not all arguments are currently supported and some may be ignored by the backend.

    client = OpenAI(
        base_url="http://localhost:8000/v1", # replace if deploying remotely
        api_key="NOT A REAL KEY",
    )
    chat_completion = client.chat.completions.create(
        model="NousResearch/Meta-Llama-3-8B-Instruct",
        messages=[{"role": "system", "content": "You are a helpful assistant."}, 
            {"role": "user", "content": "What are some of the highest rated restaurants in San Francisco?'"}],
        temperature=0.01,
        stream=True
    )

    for chat in chat_completion:
        if chat.choices[0].delta.content is not None:
            print(chat.choices[0].delta.content, end="")
    # __query_example_end__
