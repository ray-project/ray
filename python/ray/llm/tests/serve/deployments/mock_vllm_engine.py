import asyncio
import json
import random
from random import randint
from typing import Dict

from PIL import Image
from vllm.sampling_params import SamplingParams as VLLMInternalSamplingParams

from ray.llm._internal.serve.configs.error_handling import ValidationError
from ray.llm._internal.serve.configs.openai_api_models_patch import (
    ResponseFormatJsonObject,
)
from ray.llm._internal.serve.configs.server_models import (
    FinishReason,
    LogProb,
    LogProbs,
    DiskMultiplexConfig,
    LLMConfig,
    LLMRawResponse,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine_stats import (
    VLLMEngineStats,
    VLLMEngineStatTracker,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMGenerationRequest,
    VLLMSamplingParams,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
)


class MockVLLMEngine:
    def __init__(self, llm_config: LLMConfig):
        """Create a vLLM Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        assert isinstance(
            llm_config, LLMConfig
        ), f"Got invalid config {llm_config} of type {type(llm_config)}"
        self.llm_config = llm_config

        # Try to set up prompt_format when applied.
        try:
            self.llm_config.prompt_format.set_processor(
                self.llm_config.model_loading_config.model_source
            )
        except OSError:
            pass

        self._stats = VLLMEngineStatTracker()

    @staticmethod
    async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
        return InitializeNodeOutput(
            placement_group=None,
            runtime_env={},
            extra_init_kwargs={},
        )

    async def start(self):
        """No-Op"""
        return

    @staticmethod
    async def async_range(count):
        for i in range(count):
            yield i
            await asyncio.sleep(0.0)

    async def generate(self, vllm_engine_request: VLLMGenerationRequest, stream: bool):
        sampling_params = self._parse_sampling_params(
            vllm_engine_request.sampling_params
        )
        max_tokens = sampling_params.max_tokens
        if not max_tokens:
            max_tokens = randint(1, 10)
        prompt = vllm_engine_request.prompt
        prompt_len = (
            len(prompt.split()) if isinstance(prompt, str) else len(prompt.prompt)
        )
        generation_time = 0.001

        async for i in self.async_range(max_tokens):
            if i == max_tokens - 1:
                finish_reason = FinishReason.STOP
            else:
                finish_reason = None
            llm_response = LLMRawResponse(
                generated_text=f"test_{i} ",
                num_input_tokens=prompt_len,
                num_input_tokens_batch=prompt_len,
                num_generated_tokens=1,
                preprocessing_time=0,
                generation_time=generation_time,
                finish_reason=finish_reason,
                logprobs=self.get_logprobs(i, vllm_engine_request, sampling_params),
            )
            yield llm_response
            await asyncio.sleep(generation_time)

    async def check_health(self) -> bool:
        return True

    def stats(self) -> VLLMEngineStats:
        return self._stats.to_stats()

    def shutdown(self, shutdown_pg: bool = True):
        raise NotImplementedError()

    def _parse_sampling_params(
        self, sampling_params: VLLMSamplingParams
    ) -> VLLMInternalSamplingParams:
        try:
            if sampling_params.n != 1:
                raise ValueError("n>1 is not supported yet in rayllm")
            if sampling_params.logprobs:
                if sampling_params.top_logprobs:
                    if not (0 <= sampling_params.top_logprobs <= 5):
                        raise ValueError("top_logprobs must be between 0 and 5")
                    log_probs = sampling_params.top_logprobs
                else:
                    log_probs = 1
            else:
                if sampling_params.top_logprobs:
                    raise ValueError(
                        "if top_logprobs is specified, logprobs must be set to `True`"
                    )
                log_probs = None

            return VLLMInternalSamplingParams(
                n=1,
                best_of=sampling_params.best_of,
                presence_penalty=sampling_params.presence_penalty
                if sampling_params.presence_penalty is not None
                else 0.0,
                frequency_penalty=sampling_params.frequency_penalty
                if sampling_params.frequency_penalty is not None
                else 0.0,
                temperature=sampling_params.temperature
                if sampling_params.temperature is not None
                else 1.0,
                top_p=sampling_params.top_p
                if sampling_params.top_p is not None
                else 1.0,
                top_k=sampling_params.top_k
                if sampling_params.top_k is not None
                else -1,
                stop=sampling_params.stop,
                stop_token_ids=sampling_params.stop_tokens,
                ignore_eos=False,
                # vLLM will cancel internally if input+output>max_tokens
                max_tokens=sampling_params.max_tokens
                or self.llm_config.max_request_context_length,
                logprobs=log_probs,
            )
        except Exception as e:
            # Wrap the error in ValidationError so the status code
            # returned to the user is correct.
            raise ValidationError(str(e)) from e

    def get_logprobs(
        self,
        i: int,
        vllm_engine_request: VLLMGenerationRequest,
        sampling_params: VLLMSamplingParams,
    ):
        """Helper function for generating LLMRawResponse logprobs"""
        num_logprobs = sampling_params.logprobs
        top_logprobs = vllm_engine_request.sampling_params.top_logprobs
        if num_logprobs:
            log_probs = [
                LogProbs.create(
                    logprobs=[
                        LogProb(
                            logprob=0.0,
                            token=(
                                f"test_{i} " if idx == 0 else f"candidate_token_{idx}"
                            ),
                            bytes=[],
                        )
                        for idx in range(num_logprobs)
                    ],
                    top_logprobs=top_logprobs,
                )
            ]
        else:
            log_probs = None

        return log_probs


class MockEchoVLLMEngine(MockVLLMEngine):
    """
    Mock engine that responds with information about the request sent to it. Useful
    for testing the contents of VLLMGenerationRequests created in RayLLM code up to
    the vLLM boundary.
    """

    def _convert_to_json(self, vllm_engine_request: VLLMGenerationRequest) -> Dict:
        """Converts request to json.

        If the request contains an image, this method removes the image
        from `vllm_engine_request` and sets `has_image: true` in the
        output dictionary.
        This is because `Image.Image` is not json serializable.
        """
        mm_data = vllm_engine_request.multi_modal_data
        if isinstance(mm_data, dict) and "image" in mm_data:
            assert isinstance(mm_data["image"], Image.Image) or (
                isinstance(mm_data["image"], list)
                and all(
                    [
                        isinstance(image, Image.Image)
                        for image in vllm_engine_request.multi_modal_data["image"]
                    ]
                )
            ), "Image must be of type Image.Image or a list of Image.Image"
            mm_data["image"] = None
            has_image = True
        else:
            has_image = False
        res = vllm_engine_request.model_dump()
        res.update({"has_image": has_image})
        return json.dumps(res)

    async def generate(self, vllm_engine_request: VLLMGenerationRequest, stream: bool):
        yield LLMRawResponse(
            generated_text=self._convert_to_json(vllm_engine_request),
            num_input_tokens=0,
            num_input_tokens_batch=0,
            num_generated_tokens=1,
            preprocessing_time=0,
            generation_time=0.01,
            finish_reason=FinishReason.STOP,
            logprobs=None,
        )


class MockMultiplexEngine:
    def __init__(self, *args, **kwargs):
        self.started = False

    @staticmethod
    async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
        return InitializeNodeOutput(
            placement_group=None,
            runtime_env={},
            extra_init_kwargs={},
        )

    async def start(self):
        self.started = True

    async def generate(self, arg, stream):
        assert self.started, "Engine was not started"
        # First yield the arg
        yield arg
        # Yield some output
        for i in range(10):
            yield i

    async def check_health(self):
        return True


class FakeLoraModelLoader:
    async def load_model(
        self, lora_model_id: str, llm_config: LLMConfig
    ) -> DiskMultiplexConfig:
        return DiskMultiplexConfig.model_validate(
            {
                "model_id": lora_model_id,
                "max_total_tokens": llm_config.max_request_context_length,
                "local_path": "/local/path",
                "lora_assigned_int_id": 1,
            }
        )


class MockJSONModeVLLMEngine(MockVLLMEngine):
    async def generate_text(self, max_tokens, prompt_len):
        generation_time = 0.001
        async for i in self.async_range(max_tokens):
            if i == max_tokens - 1:
                finish_reason = FinishReason.STOP
            else:
                finish_reason = None
            llm_response = LLMRawResponse(
                generated_text=f"test_{i} ",
                num_input_tokens=prompt_len,
                num_input_tokens_batch=prompt_len,
                num_generated_tokens=1,
                preprocessing_time=0,
                generation_time=generation_time,
                finish_reason=finish_reason,
            )
            yield llm_response
            await asyncio.sleep(generation_time)

    async def generate_json(self, json_schema, max_tokens, prompt_len):
        random_valid_json = str(generate_from_schema(json_schema))
        # the json has double quotes where single quotes should be and single quotes where double quotes should be:
        random_valid_json = random_valid_json.replace("'", '"')

        tokens = split_string_into_chunks(random_valid_json, max_tokens)

        generation_time = 0.001
        async for i in self.async_range(max_tokens):
            finish_reason = None
            if i == max_tokens - 1:
                finish_reason = FinishReason.STOP

            generated_text = tokens[i]
            llm_response = LLMRawResponse(
                generated_text=generated_text,
                num_input_tokens=prompt_len,
                num_input_tokens_batch=prompt_len,
                num_generated_tokens=1,
                preprocessing_time=0,
                generation_time=generation_time,
                finish_reason=finish_reason,
            )
            yield llm_response
            await asyncio.sleep(generation_time)

    async def generate(self, vllm_engine_request: VLLMGenerationRequest, stream: bool):
        sampling_params = self._parse_sampling_params(
            vllm_engine_request.sampling_params
        )
        max_tokens = sampling_params.max_tokens
        if not max_tokens:
            max_tokens = randint(1, 10)
        prompt = vllm_engine_request.prompt
        prompt_len = get_prompt_length(prompt)
        response_format = sampling_params.response_format
        if response_format and isinstance(response_format, ResponseFormatJsonObject):
            response_format = sampling_params.response_format
            generator = self.generate_json(
                response_format.json_schema,
                max_tokens=max_tokens,
                prompt_len=prompt_len,
            )
        else:
            generator = self.generate_text(max_tokens=max_tokens, prompt_len=prompt_len)
        async for x in generator:
            yield x

    def _parse_sampling_params(
        self, sampling_params: VLLMSamplingParams
    ) -> VLLMInternalSamplingParams:
        new_sampling_params = super()._parse_sampling_params(sampling_params)
        new_sampling_params.response_format = sampling_params.response_format
        return new_sampling_params


def generate_from_schema(schema):
    if "type" not in schema:
        raise ValueError("Schema must have a 'type' property")

    # Check for enum and return a random value from it
    if "enum" in schema:
        return schema["enum"][0]

    if schema["type"] == "object":
        obj = {}
        for prop, prop_schema in schema.get("properties", {}).items():
            obj[prop] = generate_from_schema(prop_schema)
        return obj

    elif schema["type"] == "array":
        item_schema = schema.get("items", {})
        return [generate_from_schema(item_schema) for _ in range(random.randint(1, 3))]

    elif schema["type"] == "string":
        return "sample_string"

    elif schema["type"] == "integer":
        return random.randint(0, 100)

    elif schema["type"] == "number":
        return random.uniform(0, 100)

    elif schema["type"] == "boolean":
        return random.choice([True, False])

    else:
        raise ValueError(f"Unsupported type: {schema['type']}")


def split_string_into_chunks(s, n):
    if n <= 0:
        raise ValueError("Number of chunks must be greater than 0")

    chunk_size = len(s) // n
    remainder = len(s) % n

    chunks = []
    start = 0
    for i in range(n):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(s[start:end])
        start = end

    return chunks


def get_prompt_length(prompt):
    return len(prompt.split()) if isinstance(prompt, str) else len(prompt)
