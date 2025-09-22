import asyncio
import os
import re
import time
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TYPE_CHECKING, AsyncIterator, AsyncGenerator, List, Optional, Tuple, Union, Dict

import ray
from ray.llm._internal.serve.configs.constants import (
    MAX_NUM_TOPLOGPROBS_ALLOWED,
    MIN_NUM_TOPLOGPROBS_ALLOWED,
    RAYLLM_ENABLE_REQUEST_PROMPT_LOGS,
    RAYLLM_GUIDED_DECODING_BACKEND,
)
from ray.llm._internal.serve.configs.error_handling import (
    InputTooLong,
    ValidationError,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    FinishReason,
    GenerationRequest,
    LLMConfig,
    LLMRawResponse,
    LogProb,
    LogProbs,
    Prompt,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.deployments.llm.sglang.sglang_engine_stats import (
    ArgUsage,
    SGLangEngineStatTracker,
    usage_counters,
)
from ray.llm._internal.serve.deployments.llm.sglang.sglang_models import (
    # KV_TRANSFER_PARAMS_KEY,
    SGLangEmbeddingRequest,
    SGLangEngineConfig,
    SGLangGenerationRequest,
    SGLangSamplingParams,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
    initialize_node as initialize_node_util,
)
from ray.llm._internal.serve.deployments.utils.server_utils import floats_to_base64
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.utils import (
    LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
    ClockUnit,
    MsClock,
)
from ray.llm._internal.utils import try_import
from ray.util import metrics
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from ray.util.placement_group import (
    PlacementGroup,
    get_current_placement_group,
    placement_group,
    placement_group_table,
)

if TYPE_CHECKING:
    from vllm import SamplingParams as VLLMInternalSamplingParams
    # from vllm.config import ModelConfig, VllmConfig
    from sglang.srt.configs.model_config import ModelConfig
    from vllm.engine.arg_utils import AsyncEngineArgs

    from vllm.engine.protocol import EngineClient
    from sglang.srt.entrypoints.EngineBase import EngineBase

    from vllm.outputs import PoolingRequestOutput, RequestOutput

vllm   = try_import("vllm")  # Keep it for func make_async()
sglang = try_import("sglang")
logger = get_logger(__name__)

time_in_queue_histogram = metrics.Histogram(
    "vllm_engine_stats_time_in_queue_ms",
    "Time a request spends in the queue first forward pass not included (ms).",
    boundaries=LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
)

V1_TOO_LONG_PATTERN = re.compile(
    r".* (\d+).* is longer than the maximum model length of (\d+).*"
)


@ray.remote
class SGLangEngineWorker:
    def __init__(self, engine_kwargs: dict):
        self.engine_kwargs=engine_kwargs
        
    async def start(self):
        import torch
        gpu_ids = ray.get_gpu_ids()  
        # print(f"[DEBUG] Before, HIP_VISIBLE_DEVICES={os.environ.get("HIP_VISIBLE_DEVICES")}", flush=True)
        # os.environ["HIP_VISIBLE_DEVICES"] = ",".join(str(i) for i in gpu_ids)
        # print(f"[DEBUG] SGLang GPU HIP_VISIBLE_DEVICES={os.environ.get("HIP_VISIBLE_DEVICES")}", flush=True)
        # print(f'[DEBUG] ray.get_runtime_context(): {ray.get_runtime_context()}', flush=True)
        # print(f"[DEBUG] gpu_ids: {gpu_ids}", flush=True)
        from sglang.srt.entrypoints.engine import Engine
        self.engine = Engine(**self.engine_kwargs)

    async def generate_non_stream(self, prompt, input_ids, sampling_params, stream):
        result = await self.engine.async_generate(
                prompt=prompt,
                input_ids=input_ids,
                sampling_params=sampling_params,
                stream=stream, # This will be False
            )
        return result

    async def generate_stream(self, prompt, input_ids, sampling_params, stream):
        generator = await self.engine.async_generate(
                prompt=prompt,
                input_ids=input_ids,
                sampling_params=sampling_params,
                stream=stream,
            )
        async for chunk in generator:
            yield chunk  
            


# Note: Ray has 2 LLMEngine class. One is from server_models.py for vLLM/SGLang enum. The other one is for LLM abstract class.
class SGLangEngine(LLMEngine):
    def __init__(
        self,
        llm_config: LLMConfig,
    ):
        """Create a SGLang Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        super().__init__(llm_config)

        if sglang is None:
            raise ImportError(
                "SGLang is not installed. Please install it with `pip install ray[llm]`"
            )

        assert isinstance(
            llm_config, LLMConfig
        ), f"Got invalid config {llm_config} of type {type(llm_config)}"
        self.llm_config = llm_config
        self.engine_config = SGLangEngineConfig.from_llm_config(llm_config)

        self._stats = SGLangEngineStatTracker()
        self.running = False
        self.model_config: "ModelConfig" = None
        self.engine = None
        # self.vllm_config: "VllmConfig" = None # Does SGLang has this similar class?
        self.vllm_config = None

        # Chat template content format (openai or string)
        self._resolved_content_format = None
        # Also need local instance of the tokenizer to manage prompt formatting.
        self._tokenizer = None

        self._tokenizer_executor = ThreadPoolExecutor(max_workers=1)
        self._atokenize = vllm.utils.make_async(
            self._tokenize, executor=self._tokenizer_executor
        )

    @staticmethod
    async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
        """Run the node initializer.

        This is separate from `start` so it can run concurrently while starting the engine actor.

        It's a static method so it can be overridden for testing.
        """
        return await initialize_node_util(llm_config)

    def _tokenize( # ToDo 
        self, prompt_text: str, add_special_tokens: bool = False
    ) -> List[int]:
        encoded = self._tokenizer(prompt_text, add_special_tokens=add_special_tokens)
        return encoded.input_ids

    async def start(self):
        """Start the SGLang engine.

        If the engine is already running, do nothing.
        """

        if self.running:
            # The engine is already running!
            logger.info("Skipping engine restart because the engine is already running")
            return

        # logger.info(f"[DEBUG] self.llm_config={self.llm_config}")
        self.engine = await self._start_engine()
        self.running = True



        logger.info("Started SGLang engine.")

    async def _start_engine(self) -> "EngineBase":
        
        n_cpu = self.llm_config.deployment_config['ray_actor_options']['num_cpus']
        n_gpu = self.engine_config.num_devices
        node_initialization = await self.initialize_node(self.llm_config)    
        runtime_env = node_initialization.runtime_env # runtime_env = self.engine_config.runtime_env
        pg = node_initialization.placement_group

        # Bad allocation
        # pg = placement_group([{"CPU": n_cpu}, {"GPU": n_gpu}], strategy="STRICT_PACK")
        # ray.get(pg.ready())
        # print(f"[DEBUG] SGLang node_initialization.placement_group.bundle_specs={node_initialization.placement_group.bundle_specs}", flush=True)
        print(f"[DEBUG] SGLang placement_group_table={placement_group_table(pg)}", flush=True)
        # print("\n\n\n\n", flush=True) 
        
        from transformers import AutoTokenizer
        engine_actor = SGLangEngineWorker.options(
                num_gpus=n_gpu,
                num_cpus=0,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg,
                    placement_group_capture_child_tasks=True,
                ),
                runtime_env=runtime_env,
            ).remote(self.llm_config.engine_kwargs)
        await engine_actor.start.remote()
        self._tokenizer = AutoTokenizer.from_pretrained(self.llm_config.engine_kwargs["model_path"])
        return engine_actor

    async def prepare_request(
        self,
        request_id: str,
        prompt: Prompt,
        stream: bool,
        disk_lora_model: Optional[DiskMultiplexConfig] = None,
    ) -> GenerationRequest:
        from vllm.entrypoints.chat_utils import (
            apply_hf_chat_template as _apply_hf_chat_template,
            parse_chat_messages_futures,
        )

        model_config = self.model_config
        mm_data = None

        if isinstance(prompt.prompt, list):
            messages = [m.model_dump() for m in prompt.prompt]
            conversation, mm_futures = parse_chat_messages_futures(
                messages=messages,
                model_config=model_config,
                tokenizer=self._tokenizer,
                content_format=self._resolved_content_format,
            )
            mm_data = await mm_futures

            def apply_hf_chat_template(model_config, **kwargs):
                try:
                    return _apply_hf_chat_template(model_config=model_config, **kwargs)
                except TypeError:
                    # Legacy API before vLLM 0.9.0.
                    # TODO(#52975): Remove above once vLLM <0.9.0 is no longer supported.
                    return _apply_hf_chat_template(
                        trust_remote_code=model_config.trust_remote_code, **kwargs
                    )

            prompt_text = apply_hf_chat_template(
                model_config=model_config,
                tokenizer=self._tokenizer,
                conversation=conversation,
                chat_template=None,
                tools=None,
                tokenize=False,
                # **kwargs for tokenizer.apply_chat_template
                trust_remote_code=model_config.trust_remote_code,
                add_generation_prompt=True,
                continue_final_message=False,
            )
        else:
            prompt_text = prompt.prompt

        # Pass to SGLang tokenizer
        prompt_token_ids = await self._atokenize(prompt_text)
        logger.debug(f"[DEBUG] prompt_text={prompt_text}")
        logger.debug(f"[DEBUG] prompt_token_ids={prompt_token_ids}")

        request_params = {
            "prompt": prompt_text,
            "prompt_token_ids": prompt_token_ids,
            "request_id": request_id,
            "sampling_params": SGLangSamplingParams.from_prompt(prompt),
            "disk_multiplex_config": disk_lora_model,
            "stream": stream,
        }
        if mm_data:
            request_params["multi_modal_data"] = mm_data

        sglang_request = SGLangGenerationRequest(**request_params)
        return sglang_request
    
    def trim_overlap(self, existing_text, new_chunk):
        """
        Finds the largest suffix of 'existing_text' that is a prefix of 'new_chunk'
        and removes that overlap from the start of 'new_chunk'.
        """
        max_overlap = 0
        max_possible = min(len(existing_text), len(new_chunk))
        for i in range(max_possible, 0, -1):
            if existing_text.endswith(new_chunk[:i]):
                max_overlap = i
                break
        return new_chunk[max_overlap:]

    async def generate(
        self, request: GenerationRequest
    ) -> AsyncGenerator[LLMRawResponse, None]:
        """Generate an LLMRawResponse stream

        The SGLang generation request will be passed into SGLang, and the resulting output
        will be wrapped in an LLMRawResponse and yielded back to the user.

        """
        if RAYLLM_ENABLE_REQUEST_PROMPT_LOGS:
            logger.info(
                f"Request {request.request_id} started. " f"Prompt: {request.prompt}"
            )
        

        # if request.prompt_token_ids is not None:
        #     prompt = vllm.inputs.TokensPrompt(
        #         prompt_token_ids=request.prompt_token_ids,
        #         multi_modal_data=request.multi_modal_data,
        #     )
        # else:
        #     prompt = vllm.inputs.TextPrompt(
        #         prompt=request.prompt,
        #         multi_modal_data=request.multi_modal_data,
        #     )

        # Construct a results generator from SGLang
        sampling_params_dict=self._parse_sampling_params(request.sampling_params)

        if request.stream == False:
            result = await self.engine.generate_non_stream.remote(
                prompt=request.prompt,
                input_ids=request.prompt_token_ids,
                sampling_params=sampling_params_dict,
                stream=False,
            )
            logger.debug(f"[DEBUG] result={result}", flush=True)
            meta_info = result['meta_info']
            clock = MsClock(unit=ClockUnit.s)
            yield LLMRawResponse(
                generated_text=result['text'],
                num_generated_tokens=meta_info['completion_tokens'] - meta_info['prompt_tokens'],
                # logprobs=log_probs,
                num_generated_tokens_batch=meta_info['completion_tokens'] - meta_info['prompt_tokens'],
                num_input_tokens=meta_info['prompt_tokens'],
                num_input_tokens_batch=meta_info['prompt_tokens'],
                preprocessing_time=0,
                generation_time=clock.reset_interval(),
                finish_reason=meta_info['finish_reason']['type'],
                metadata=meta_info,
            )
        else:
            final_text = ""
            clock = MsClock(unit=ClockUnit.s)
            for ref in self.engine.generate_stream.remote(
                    prompt=request.prompt,
                    input_ids=request.prompt_token_ids,
                    sampling_params=sampling_params_dict,
                    stream=True,
                ):
                chunk = ray.get(ref)
                chunk_text = chunk["text"]
                meta_info = chunk['meta_info']
                # print(f"[DEBUG] chunk = {chunk}", flush=True)
                finish_reason = meta_info["finish_reason"]["type"] if meta_info["finish_reason"] else None
                cleaned_chunk = self.trim_overlap(final_text, chunk_text)
                final_text += cleaned_chunk
                
                
                yield LLMRawResponse(
                    generated_text=final_text,
                    num_generated_tokens=meta_info['completion_tokens'],
                    # logprobs=log_probs,
                    num_generated_tokens_batch=meta_info['completion_tokens'],
                    num_input_tokens=meta_info['prompt_tokens'],
                    num_input_tokens_batch=meta_info['prompt_tokens'],
                    preprocessing_time=0,
                    generation_time=clock.reset_interval(),
                    finish_reason=finish_reason,
                    metadata=meta_info,
                )
           
        

        # # Loop over the results
        # num_text_returned = 0
        # all_tokens_collected = 0
        # clock = MsClock(unit=ClockUnit.s)
        # log_probs_idx = 0
        # finish_reason = None
        # num_input_tokens = 0
        # try:
        #     print(f"[DEBUG] wait for server response")
        #     start = time.perf_counter()
        #     request_output = None
        #     async for request_output in self._stats.auto_track(results_generator):
        #         # TODO(tchordia): handle more than one output
        #         assert (
        #             len(request_output.outputs) == 1
        #         ), "Received more than 1 output from sglang, aborting"

        #         output = request_output.outputs[0]
        #         text_output = output.text[num_text_returned:]
        #         num_text_returned += len(text_output)
        #         num_input_tokens = len(request_output.prompt_token_ids)
        #         tokens_collected = len(output.token_ids) - all_tokens_collected
        #         all_tokens_collected += tokens_collected
        #         finish_reason = FinishReason.from_vllm_finish_reason(
        #             output.finish_reason
        #         )

        #         self._handle_input_too_long(request_output, finish_reason)

        #         log_probs, log_probs_idx = self._extract_logprobs(
        #             output,
        #             log_probs_idx,
        #             request.sampling_params.top_logprobs,
        #         )
        #         internal_metadata = {}
        #         if getattr(request_output, "kv_transfer_params", None) is not None:
        #             internal_metadata[
        #                 KV_TRANSFER_PARAMS_KEY
        #             ] = request_output.kv_transfer_params
        #         yield LLMRawResponse(
        #             generated_text=text_output,
        #             num_generated_tokens=tokens_collected,
        #             logprobs=log_probs,
        #             num_generated_tokens_batch=tokens_collected,
        #             num_input_tokens=num_input_tokens,
        #             num_input_tokens_batch=num_input_tokens,
        #             preprocessing_time=0,
        #             generation_time=clock.reset_interval(),
        #             finish_reason=finish_reason,
        #             metadata=internal_metadata,
        #         )

        #     if request_output is not None:
        #         total_request_time = time.perf_counter() - start
        #         if request_output.metrics is None:
        #             # vLLM V1 metrics are not included in the request output yet.
        #             queue_time = "N/A"
        #             generation_time_str = "N/A"
        #             tokens_s = "N/A"
        #             generated_tokens_s = "N/A"
        #         else:
        #             time_in_queue_histogram.observe(
        #                 request_output.metrics.time_in_queue
        #             )
        #             queue_time = f"{request_output.metrics.time_in_queue}s"
        #             generation_time = (
        #                 total_request_time - request_output.metrics.time_in_queue
        #             )
        #             generation_time_str = f"{generation_time}s"
        #             tokens_s = (
        #                 num_input_tokens + all_tokens_collected
        #             ) / generation_time
        #             generated_tokens_s = all_tokens_collected / generation_time

        #         logger.info(
        #             f"Request {request.request_id} finished ({finish_reason}). "
        #             f"Total time: {total_request_time}s, "
        #             f"Queue time: {queue_time}, "
        #             f"Generation+async time: {generation_time_str}, "
        #             f"Input tokens: {num_input_tokens}, "
        #             f"Generated tokens: {all_tokens_collected}, "
        #             f"tokens/s: {tokens_s}, "
        #             f"generated tokens/s: {generated_tokens_s}."
        #         )
        #     else:
        #         logger.warning(
        #             f"Request {request.request_id} "
        #             "finished without any output. "
        #             f"Input tokens: {num_input_tokens}."
        #         )
        # except ValueError as e:
        #     error_args = e.args
        #     if len(error_args) == 3 and "Input too long." == error_args[0]:
        #         _, input_length, max_input_length = error_args
        #         raise InputTooLong(input_length, max_input_length).exception from None
        #     elif len(error_args) == 1 and V1_TOO_LONG_PATTERN.match(error_args[0]):
        #         parsed_error = V1_TOO_LONG_PATTERN.match(error_args[0])
        #         raise InputTooLong(
        #             int(parsed_error[1]), int(parsed_error[2])
        #         ).exception from None
        #     else:
        #         raise e from None
        # finally:
        #     # Ensure that we cancel on the engine once we have exited the streaming
        #     # phase
        #     await self.engine.abort(request.request_id)

    def _get_prompt_limit(self) -> int:
        """Helper to get the prompt limit from scheduler config

        Port from https://github.com/vllm-project/vllm/blob/7b5ecf79bd94aab0d782c70126d0dcc37c16bc60/vllm/core/scheduler.py#L939
        """
        scheduler_config = self.vllm_config.scheduler_config
        if (
            scheduler_config.chunked_prefill_enabled
            and not scheduler_config.is_multi_step
        ):
            prompt_limit = scheduler_config.max_model_len
        else:
            prompt_limit = min(
                scheduler_config.max_model_len,
                scheduler_config.max_num_batched_tokens,
            )
        return prompt_limit

    def _handle_input_too_long(
        self, request_output: "RequestOutput", finish_reason: Optional[FinishReason]
    ):
        if (
            finish_reason
            and finish_reason == FinishReason.LENGTH
            and hasattr(request_output.metrics, "first_token_time")
            and request_output.metrics.first_token_time is None
        ):
            # This means that the prompt was too long and we did not generate anything.
            raise InputTooLong(
                len(request_output.prompt_token_ids), self._get_prompt_limit()
            ).exception

    async def embed(
        self, sglang_embedding_request: SGLangEmbeddingRequest
    ) -> Tuple[List[List[float]], int]:
        """Return (embeddings, num_prompt_tokens)"""

        num_prompts = len(sglang_embedding_request.prompt)
        if RAYLLM_ENABLE_REQUEST_PROMPT_LOGS:
            logger.info(
                f"Encoding request {sglang_embedding_request.request_id} started. "
                f"Num prompts: {num_prompts}"
            )

        generators: List[AsyncGenerator["PoolingRequestOutput", None]] = []

        prompts = sglang_embedding_request.prompt
        if isinstance(prompts, str):
            prompts = [prompts]

        for i, prompt in enumerate(prompts):
            request_id = f"{sglang_embedding_request.request_id}-{i}"
            gen: AsyncGenerator["PoolingRequestOutput", None] = self.engine.encode(
                prompt=vllm.inputs.TextPrompt(
                    prompt=prompt,
                ),
                pooling_params=vllm.pooling_params.PoolingParams(),
                request_id=request_id,
                lora_request=sglang_embedding_request.lora_request,  # type: ignore
            )
            generators.append(gen)

        embedding_data = []
        total_prompt_tokens = 0

        for gen in generators:
            async for result in gen:
                embedding = result.outputs.embedding
                if sglang_embedding_request.encoding_format == "base64":
                    embedding = floats_to_base64(embedding)

                embedding_data.append(embedding)
                total_prompt_tokens += len(result.prompt_token_ids)

        return embedding_data, total_prompt_tokens

    # dummy check health
    async def check_health(self) -> None:
        from fastapi import FastAPI, Response
        return Response(status_code=200)

        # if not hasattr(self.engine, "check_health"):
        #     raise RuntimeError(f"{type(self.engine)} does not support health check.")

        # try:
        #     return await asyncio.wait_for(self.engine.check_health(), timeout=15)
        # except BaseException as e:
        #     logger.exception("Healthcheck failed. The replica will be restarted")
        #     raise e from None

    @staticmethod
    def _collect_usage_metrics(sampling_params: SGLangSamplingParams) -> None:
        if sampling_params.best_of is not None:
            usage_counters[ArgUsage.BEST_OF].inc()

        if sampling_params.presence_penalty is not None:
            usage_counters[ArgUsage.PRESENCE_PENALTY].inc()

        if sampling_params.frequency_penalty is not None:
            usage_counters[ArgUsage.FREQUENCY_PENALTY].inc()

        if (
            sampling_params.presence_penalty is not None
            and sampling_params.frequency_penalty is not None
        ):
            usage_counters[ArgUsage.PRESENCE_AND_FREQUENCY_PENALTY].inc()

        if sampling_params.temperature is not None:
            usage_counters[ArgUsage.TEMPERATURE].inc()

        if sampling_params.top_p is not None:
            usage_counters[ArgUsage.TOP_P].inc()

        if sampling_params.top_k is not None:
            usage_counters[ArgUsage.TOP_K].inc()

        if sampling_params.stop is not None:
            usage_counters[ArgUsage.STOP].inc()

        if sampling_params.max_tokens is not None:
            usage_counters[ArgUsage.MAX_TOKENS].inc() # ToDo: MAX_TOKENS -> MAX__NEW_TOKENS

        if sampling_params.logprobs is not None:
            usage_counters[ArgUsage.LOGPROBS].inc()

    # check keyword from sglang/python/sglang/srt/sampling/sampling_params.py
    def _parse_sampling_params(
        self, sampling_params: SGLangSamplingParams
    ) -> dict:
        """Parse the sampling parameters from the prompt.
        This function is used to parse the sampling parameters from the prompt.
        It also collects the usage metrics for the sampling parameters.
        Args:
            sampling_params: The sampling parameters defined in ray.serve.llm.
        Returns:
            dict, The parsed sampling parameters.
        """
        self._collect_usage_metrics(sampling_params)
        try:
            # if self.model_config is None:
            #     raise RuntimeError(
            #         "SGLangEngine.model_config not set. Maybe SGLangEngine.start() was not called?"
            #     )

            # log_probs = None
            # if sampling_params.logprobs:
            #     max_logprobs = getattr(self.model_config, "max_logprobs", 0)
            #     max_logprobs = min(MAX_NUM_TOPLOGPROBS_ALLOWED, max_logprobs)
            #     if max_logprobs == 0:
            #         raise ValueError("This model doesn't support outputting logprobs.")
            #     if sampling_params.top_logprobs:
            #         if not (
            #             MIN_NUM_TOPLOGPROBS_ALLOWED
            #             <= sampling_params.top_logprobs
            #             <= max_logprobs
            #         ):
            #             raise ValueError(
            #                 f"top_logprobs must be between {MIN_NUM_TOPLOGPROBS_ALLOWED} "
            #                 f"and {max_logprobs}. Got {sampling_params.top_logprobs}."
            #             )
            #         log_probs = sampling_params.top_logprobs
            #     else:
            #         log_probs = 1
            # else:
            #     if sampling_params.top_logprobs:
            #         raise ValueError(
            #             "if top_logprobs is specified, logprobs must be set to `True`"
            #         )
                    
            kwargs = dict(
                n=1,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                repetition_penalty=1.0,
                temperature=1.0,
                top_p=1.0,
                top_k=-1,
                stop=sampling_params.stop,
                stop_token_ids=sampling_params.stop_tokens,
                ignore_eos=False,
            )
            if sampling_params.presence_penalty is not None:
                kwargs["presence_penalty"] = sampling_params.presence_penalty
            if sampling_params.frequency_penalty is not None:
                kwargs["frequency_penalty"] = sampling_params.frequency_penalty
            if sampling_params.repetition_penalty is not None:
                kwargs["repetition_penalty"] = sampling_params.repetition_penalty
            if sampling_params.temperature is not None:
                kwargs["temperature"] = sampling_params.temperature
            if sampling_params.top_p is not None:
                kwargs["top_p"] = sampling_params.top_p
            if sampling_params.top_k is not None:
                kwargs["top_k"] = sampling_params.top_k
            if sampling_params.ignore_eos is not None:
                kwargs["ignore_eos"] = sampling_params.ignore_eos
            if sampling_params.max_tokens is not None:
                kwargs["max_new_tokens"] = sampling_params.max_tokens 
            
            # print(f"[DEBUG] sampling_params={sampling_params}", flush=True)

            return kwargs
        except Exception as e:
            # Wrap the error in ValidationError so the status code
            # returned to the user is correct.
            raise ValidationError(str(e)) from e

    @staticmethod
    def _extract_logprobs(
        output: "RequestOutput",
        log_probs_idx: int,
        top_logprobs: Optional[int] = None,
    ) -> Tuple[List[LogProbs], int]:
        all_log_probs = output.logprobs[log_probs_idx:] if output.logprobs else None
        return_log_probs = []
        if all_log_probs:
            for log_probs in all_log_probs:
                log_probs_for_n_sampled = [
                    LogProb(
                        logprob=log_prob.logprob,
                        token=log_prob.decoded_token,
                        bytes=list(log_prob.decoded_token.encode()),
                    )
                    for log_prob in log_probs.values()
                    if log_prob.decoded_token is not None
                ]
                if log_probs_for_n_sampled:
                    return_log_probs += [
                        LogProbs.create(
                            logprobs=log_probs_for_n_sampled, top_logprobs=top_logprobs
                        )
                    ]
        return return_log_probs, log_probs_idx + len(return_log_probs)
