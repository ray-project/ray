import os
import re
import time
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TYPE_CHECKING, AsyncGenerator, List, Optional, Tuple

import ray
from ray.llm._internal.common.utils.import_utils import try_import
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
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine_stats import (
    ArgUsage,
    VLLMEngineStatTracker,
    usage_counters,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    KV_TRANSFER_PARAMS_KEY,
    VLLMEmbeddingRequest,
    VLLMEngineConfig,
    VLLMGenerationRequest,
    VLLMSamplingParams,
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
from ray.util import metrics
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

if TYPE_CHECKING:
    from vllm import SamplingParams as VLLMInternalSamplingParams
    from vllm.config import ModelConfig, VllmConfig
    from vllm.engine.arg_utils import AsyncEngineArgs
    from vllm.engine.protocol import EngineClient
    from vllm.outputs import PoolingRequestOutput, RequestOutput

vllm = try_import("vllm")
logger = get_logger(__name__)

time_in_queue_histogram = metrics.Histogram(
    "vllm_engine_stats_time_in_queue_ms",
    "Time a request spends in the queue first forward pass not included (ms).",
    boundaries=LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
)

V1_TOO_LONG_PATTERN = re.compile(
    r".* (\d+).* is longer than the maximum model length of (\d+).*"
)


def _get_async_engine_args(llm_config: LLMConfig) -> "AsyncEngineArgs":
    engine_config = llm_config.get_engine_config()

    # This `model` is the local path on disk, or the hf model id.
    # If it is the hf_model_id, vLLM automatically downloads the correct model from HF.
    # We want this to be the local path on the disk when we already downloaded the
    # model artifacts from a remote storage during node initialization,
    # so vLLM will not require HF token for it and try to download it again.
    model = engine_config.actual_hf_model_id
    if isinstance(llm_config.model_loading_config.model_source, str):
        model = llm_config.model_loading_config.model_source

    return vllm.AsyncEngineArgs(
        **{
            "model": model,
            "distributed_executor_backend": "ray",
            "guided_decoding_backend": RAYLLM_GUIDED_DECODING_BACKEND,
            "disable_log_stats": False,
            **engine_config.get_initialization_kwargs(),
        }
    )


def _get_vllm_engine_config(
    llm_config: LLMConfig,
) -> Tuple["AsyncEngineArgs", "VllmConfig"]:
    async_engine_args = _get_async_engine_args(llm_config)
    vllm_config = async_engine_args.create_engine_config()
    return async_engine_args, vllm_config


def _clear_current_platform_cache():
    """Clear the cache of the current platform.

    vllm current has an lru cache for getting device compatibility
    that will not have the correct returned value if
    CUDA_VISIBLE_DEVICES is not set properly. In RayLLM eventually
    when we want to create the engine the env will be set properly,
    but till then, upon the import of vllm somewhere
    (which is a mystery) the lru cache will have the wrong value.
    This function will clear the cache so that the next time the
    cache is accessed, it will be re-evaluated.

    Related issues:
    https://github.com/vllm-project/vllm/issues/8402
    https://github.com/vllm-project/vllm/issues/7890
    """
    from vllm.platforms import current_platform

    # TODO(seiji): remove this once https://github.com/vllm-project/vllm/pull/18979 is merged
    if (
        "CUDA_VISIBLE_DEVICES" in os.environ
        and os.environ["CUDA_VISIBLE_DEVICES"] == ""
    ):
        del os.environ["CUDA_VISIBLE_DEVICES"]

    # This check is just to future proof this implementation
    # in case vllm removes their lru_cache decorator
    if hasattr(current_platform.get_device_capability, "cache_clear"):
        logger.info("Clearing the current platform cache ...")
        current_platform.get_device_capability.cache_clear()


class _EngineBackgroundProcess:
    def __init__(self, ipc_path, engine_args, engine_config):
        from vllm.engine.multiprocessing.engine import MQLLMEngine
        from vllm.plugins import load_general_plugins
        from vllm.usage.usage_lib import UsageContext

        # Adapted from vllm.engine.multiprocessing.engine.MQLLMEngine.from_engine_args
        load_general_plugins()

        # Note (genesu): There is a bug in vllm 0.7.2 forced the use of uni processing
        # executor when world_size is 1. This is a bug in vllm 0.7.2 and
        # is fixed by https://github.com/vllm-project/vllm/pull/12934 which is shipped
        # with vllm 0.7.3. However, in Ray's llm package, we will enforce the use of
        # ray distributed executor for all cases so it's always compatible with Ray.
        from vllm.executor.ray_distributed_executor import RayDistributedExecutor

        # Clear the cache of the current platform.
        _clear_current_platform_cache()

        self.engine = MQLLMEngine(
            ipc_path=ipc_path,
            use_async_sockets=engine_config.model_config.use_async_output_proc,
            vllm_config=engine_config,
            executor_class=RayDistributedExecutor,
            log_requests=not engine_args.disable_log_requests,
            log_stats=not engine_args.disable_log_stats,
            usage_context=UsageContext.API_SERVER,
        )
        self._error = None

    def start(self):
        try:
            self.engine.start()
        except Exception as e:
            self._error = e

    def get_error(self):
        return self._error


class VLLMEngine(LLMEngine):
    def __init__(
        self,
        llm_config: LLMConfig,
    ):
        """Create a vLLM Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        super().__init__(llm_config)

        if vllm is None:
            raise ImportError(
                "vLLM is not installed. Please install it with `pip install ray[llm]`."
            )
        from vllm import envs as vllm_envs, utils as vllm_utils

        # Pick a random port in P/D case.
        kv_transfer_config = llm_config.engine_kwargs.get("kv_transfer_config", None)
        if kv_transfer_config is not None:
            if not vllm_envs.VLLM_USE_V1:
                logger.warning("Ray Serve LLM only supports P/D with v1 vLLM engine.")
            connector_type = getattr(kv_transfer_config, "kv_connector", "")
            if connector_type != "NixlConnector":
                raise ValueError("Only NixlConnector is supported for kv transfer.")
            if (
                "VLLM_NIXL_SIDE_CHANNEL_PORT" not in vllm_envs.environment_variables
                or "VLLM_NIXL_SIDE_CHANNEL_HOST" not in vllm_envs.environment_variables
            ):
                raise ValueError(
                    "This vLLM version does not support VLLM_NIXL_SIDE_CHANNEL_PORT"
                    "or VLLM_NIXL_SIDE_CHANNEL_HOST environment variable. It's likely"
                    "that you are using an older version of vLLM."
                )

            if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_PORT"):
                port: int = vllm_utils.get_open_port()
                os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] = str(port)
            if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_HOST"):
                os.environ["VLLM_NIXL_SIDE_CHANNEL_HOST"] = vllm_utils.get_ip()

            # We need to overwrite the engine_id to make it unique across replicas.
            engine_id = getattr(kv_transfer_config, "engine_id", str(uuid.uuid4()))
            host = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_HOST
            port = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_PORT
            kv_transfer_config.engine_id = "-".join([engine_id, host, str(port)])

        assert isinstance(
            llm_config, LLMConfig
        ), f"Got invalid config {llm_config} of type {type(llm_config)}"
        self.llm_config = llm_config
        self.engine_config = VLLMEngineConfig.from_llm_config(llm_config)

        self._stats = VLLMEngineStatTracker()
        self.running = False
        self.model_config: "ModelConfig" = None
        self.engine = None
        self.vllm_config: "VllmConfig" = None

        # Chat template content format (openai or string)
        self._resolved_content_format = None
        # Also need local instance of the tokenizer to manage prompt formatting.
        self._tokenizer = None

        self._tokenizer_executor = ThreadPoolExecutor(max_workers=1)
        self._atokenize = vllm_utils.make_async(
            self._tokenize, executor=self._tokenizer_executor
        )

    @staticmethod
    async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
        """Run the node initializer.

        This is separate from `start` so it can run concurrently while starting the engine actor.

        It's a static method so it can be overridden for testing.
        """
        return await initialize_node_util(llm_config)

    def _tokenize(
        self, prompt_text: str, add_special_tokens: bool = False
    ) -> List[int]:
        encoded = self._tokenizer(prompt_text, add_special_tokens=add_special_tokens)
        return encoded.input_ids

    async def start(self):
        """Start the vLLM engine.

        If the engine is already running, do nothing.
        """
        from vllm.entrypoints.chat_utils import (
            resolve_chat_template_content_format as _resolve_chat_template_content_format,
        )

        if self.running:
            # The engine is already running!
            logger.info("Skipping engine restart because the engine is already running")
            return

        self.engine = await self._start_engine()
        self.running = True
        self.model_config = await self.engine.get_model_config()

        self._tokenizer = await self.engine.get_tokenizer()

        def resolve_chat_template_content_format(model_config, **kwargs):
            try:
                return _resolve_chat_template_content_format(
                    model_config=model_config, **kwargs
                )
            except TypeError:
                # Legacy API before vLLM 0.9.0.
                # TODO(#52975): Remove this try-except once vLLM <0.9.0 is no longer supported.
                return _resolve_chat_template_content_format(
                    trust_remote_code=model_config.trust_remote_code, **kwargs
                )

        self._resolved_content_format = resolve_chat_template_content_format(
            model_config=self.model_config,
            # Use HF to get the chat template so set it to None here.
            chat_template=None,
            # Default to None, change when it's needed.
            # vLLM does not have a high level API to support all of this.
            tools=None,
            # Let vLLM decide the content format.
            given_format="auto",
            tokenizer=self._tokenizer,
        )

        logger.info("Started vLLM engine.")

    async def _start_engine(self) -> "EngineClient":
        from vllm import envs as vllm_envs

        # Since vLLM 0.8.0, the logic to determine v0/v1 engine is as follows:
        # 1. If VLLM_USE_V1 is not set, then it tries to use v1 engine. However,
        #    if any feature specified in the engine config is not supported, then
        #    it falls back to v0. Note that launching vLLM on a non-main thread
        #    is an experimental feature, so vLLM will fall back to v0 in this case.
        # 2. If VLLM_USE_V1 is set to 1, then it will use v1 engine even with
        #    experimental features (such as launching vLLM on a non-main thread).
        # 3. If VLLM_USE_V1 is set to 0, force using v0 engine.
        # In Ray Serve LLM, we forbid case 1 because we have to know exactly which engine is used.
        if not vllm_envs.is_set("VLLM_USE_V1"):
            logger.warning(
                "VLLM_USE_V1 environment variable is not set, using vLLM v0 as default. "
                "Later we may switch default to use v1 once vLLM v1 is mature."
            )
            vllm_envs.set_vllm_use_v1(False)

        if not vllm_envs.VLLM_USE_V1:
            if self.llm_config.log_engine_metrics:
                raise ValueError("V1 vLLM Engine is required to log engine metrics")

            return await self._start_engine_v0()

        return await self._start_engine_v1()

    async def _prepare_engine_config(self, use_v1: bool):
        """
        Prepare the engine config to start the engine.

        Args:
            use_v1: Whether to use vLLM V1 engine.

        Returns:
            engine_args: The engine arguments.
            engine_config: The engine configuration.
            node_initialization: The node initialization.
        """
        # Initialize node and return all configurations
        node_initialization = await self.initialize_node(self.llm_config)

        if self.engine_config.use_gpu:
            # Create engine config on a task with access to GPU,
            # as GPU capability may be queried.
            ref = (
                ray.remote(
                    num_cpus=0,
                    num_gpus=1,
                    accelerator_type=self.llm_config.accelerator_type,
                )(_get_vllm_engine_config)
                .options(
                    runtime_env=node_initialization.runtime_env,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=node_initialization.placement_group,
                    ),
                )
                .remote(self.llm_config)
            )
            engine_args, engine_config = ray.get(ref)
        else:
            engine_args, engine_config = _get_vllm_engine_config(self.llm_config)

        # Note (genesu): vllm_config is used to extract the scheduler config for
        # computing the correct prompt limit.
        self.vllm_config = engine_config
        return engine_args, engine_config, node_initialization

    async def _start_engine_v1(self) -> "EngineClient":
        """Start the vLLM v1 engine. Note that we only use _get_async_engine_args
        to get the engine args and don't use _get_vllm_engine_config, because
        we integrate vLLM v1 using the highest-level async engine API.
        TODO: Refactor vLLM v0 integration to use the same async engine API
        to simplify the code.
        """
        (
            engine_args,
            engine_config,
            node_initialization,
        ) = await self._prepare_engine_config(use_v1=True)

        return self._start_async_llm_engine(
            engine_args,
            engine_config,
            node_initialization.placement_group,
            use_v1=True,
        )

    async def _start_engine_v0(self) -> "EngineClient":
        from vllm.engine.multiprocessing.client import MQLLMEngineClient

        (
            engine_args,
            engine_config,
            node_initialization,
        ) = await self._prepare_engine_config(use_v1=False)

        if MQLLMEngineClient.is_unsupported_config(engine_config):
            # If the engine is not supported, we fall back to the legacy async engine.
            #
            # Note (genesu): as of 2025-02-11, this code path is only triggered when
            # pipeline parallelism is > 1. And this is due to the vllm mq engine have
            # not implemented the pipeline parallelism yet.
            return self._start_async_llm_engine(
                engine_args,
                engine_config,
                node_initialization.placement_group,
                use_v1=False,
            )

        return await self._start_mq_engine(
            engine_args, engine_config, node_initialization.placement_group
        )

    async def _start_mq_engine(
        self,
        engine_args: "AsyncEngineArgs",
        engine_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":
        from vllm.engine.multiprocessing.client import MQLLMEngineClient
        from vllm.utils import get_open_zmq_ipc_path

        ipc_path = get_open_zmq_ipc_path()

        BackgroundCls = ray.remote(
            num_cpus=0,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group,
                placement_group_capture_child_tasks=True,
            ),
            runtime_env=dict(
                env_vars=dict(
                    VLLM_USE_V1="0",
                ),
            ),
        )(_EngineBackgroundProcess)
        # Run the process in the background
        process_ref = BackgroundCls.remote(ipc_path, engine_args, engine_config)
        process_ref.start.remote()
        engine_client = MQLLMEngineClient(
            ipc_path=ipc_path,
            engine_config=engine_config,
            engine_pid=os.getpid(),
        )

        logger.info("[STATUS] Getting the server ready ...")
        while True:
            try:
                await engine_client.setup()
                break
            except TimeoutError:
                # A timeout is raised if client cannot connect to the background process.
                # This could be due to one of the following reasons:
                # 1. The engine has died during construction of the actor: In this case
                # get() on any of its methods will raise an ActorDiedError which should
                # be re-raised
                # 2. The engine is just not up yet (downloading the model, sharding, etc.)
                # In this case, we should just wait.
                # 3. Something in the .start() has caused the engine to fail: In this
                # case the exception is caught and get_error will return the error
                # which should be re-raised.
                logger.info("[STATUS] Waiting for engine process ...")
                try:
                    # Wait 1 second to get any potential error raised in the engine loop
                    err = ray.get(process_ref.get_error.remote(), timeout=1)
                    if err:
                        raise RuntimeError("Background Engine loop is dead.") from err
                except ray.exceptions.GetTimeoutError:
                    # If it times out then the background loop is keeping it busy
                    pass
                except ray.exceptions.ActorDiedError as e:
                    logger.error("[ERROR] Actor died.")
                    raise RuntimeError("Background Engine loop is dead.") from e

        logger.info("[STATUS] Server is ready.")

        return engine_client

    def _start_async_llm_engine(
        self,
        engine_args: "AsyncEngineArgs",
        vllm_config: "VllmConfig",
        placement_group: PlacementGroup,
        use_v1: bool = False,
    ) -> "EngineClient":
        """Creates an async LLM engine from the engine arguments."""
        from vllm.v1.executor.abstract import Executor

        vllm_config.parallel_config.placement_group = placement_group

        _clear_current_platform_cache()

        custom_stat_loggers = None
        if self.llm_config.log_engine_metrics:
            from ray.llm._internal.serve.deployments.llm.vllm.vllm_loggers import (
                RayPrometheusStatLogger,
            )

            # V1 AsyncLLMEngine does not yet support add_logger
            # For now, assume folks enabling log_engine_metrics do not require LoggingStatLogger, PrometheusStatLogger
            custom_stat_loggers = [RayPrometheusStatLogger]

        executor_class = Executor.get_class(vllm_config)
        logger.info(f"Using executor class: {executor_class}")
        engine = vllm.AsyncLLMEngine(
            vllm_config=vllm_config,
            executor_class=executor_class,
            log_stats=not engine_args.disable_log_stats,
            stat_loggers=custom_stat_loggers,
        )

        return engine

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

        prompt_token_ids = await self._atokenize(prompt_text)

        request_params = {
            "prompt": prompt_text,
            "prompt_token_ids": prompt_token_ids,
            "request_id": request_id,
            "sampling_params": VLLMSamplingParams.from_prompt(prompt),
            "disk_multiplex_config": disk_lora_model,
            "stream": stream,
        }
        if mm_data:
            request_params["multi_modal_data"] = mm_data

        vllm_request = VLLMGenerationRequest(**request_params)
        return vllm_request

    async def generate(
        self, request: GenerationRequest
    ) -> AsyncGenerator[LLMRawResponse, None]:
        """Generate an LLMRawResponse stream

        The vLLM generation request will be passed into vLLM, and the resulting output
        will be wrapped in an LLMRawResponse and yielded back to the user.

        Error handling:

        We schedule a finalizer that will abort the request on the engine.

        If an exception is raised in this function or vllm, the finalizer guarantees that the request is aborted.
        If an exception is raised in the caller, when this generator is gced, it will run the finalizer and abort the request.

        This should also handle the case where the caller is cancelled (raises asyncio.CancelledError)
        """
        if RAYLLM_ENABLE_REQUEST_PROMPT_LOGS:
            logger.info(
                f"Request {request.request_id} started. " f"Prompt: {request.prompt}"
            )

        if request.prompt_token_ids is not None:
            prompt = vllm.TokensPrompt(
                prompt_token_ids=request.prompt_token_ids,
                multi_modal_data=request.multi_modal_data,
            )
        else:
            prompt = vllm.TextPrompt(
                prompt=request.prompt,
                multi_modal_data=request.multi_modal_data,
            )

        # Construct a results generator from vLLM
        results_generator: AsyncGenerator["RequestOutput", None] = self.engine.generate(
            prompt=prompt,
            sampling_params=self._parse_sampling_params(request.sampling_params),
            request_id=request.request_id,
            lora_request=request.lora_request,  # type: ignore
        )

        # Loop over the results
        num_text_returned = 0
        all_tokens_collected = 0
        clock = MsClock(unit=ClockUnit.s)
        log_probs_idx = 0
        finish_reason = None
        num_input_tokens = 0
        try:
            start = time.perf_counter()
            request_output = None
            async for request_output in self._stats.auto_track(results_generator):
                # TODO(tchordia): handle more than one output
                assert (
                    len(request_output.outputs) == 1
                ), "Received more than 1 output from vllm, aborting"

                output = request_output.outputs[0]
                text_output = output.text[num_text_returned:]
                num_text_returned += len(text_output)
                num_input_tokens = len(request_output.prompt_token_ids)
                tokens_collected = len(output.token_ids) - all_tokens_collected
                all_tokens_collected += tokens_collected
                finish_reason = FinishReason.from_vllm_finish_reason(
                    output.finish_reason
                )

                self._handle_input_too_long(request_output, finish_reason)

                log_probs, log_probs_idx = self._extract_logprobs(
                    output,
                    log_probs_idx,
                    request.sampling_params.top_logprobs,
                )
                internal_metadata = {}
                if getattr(request_output, "kv_transfer_params", None) is not None:
                    internal_metadata[
                        KV_TRANSFER_PARAMS_KEY
                    ] = request_output.kv_transfer_params
                yield LLMRawResponse(
                    generated_text=text_output,
                    num_generated_tokens=tokens_collected,
                    logprobs=log_probs,
                    num_generated_tokens_batch=tokens_collected,
                    num_input_tokens=num_input_tokens,
                    num_input_tokens_batch=num_input_tokens,
                    preprocessing_time=0,
                    generation_time=clock.reset_interval(),
                    finish_reason=finish_reason,
                    metadata=internal_metadata,
                )

            if request_output is not None:
                total_request_time = time.perf_counter() - start
                if request_output.metrics is None:
                    # vLLM V1 metrics are not included in the request output yet.
                    queue_time = "N/A"
                    generation_time_str = "N/A"
                    tokens_s = "N/A"
                    generated_tokens_s = "N/A"
                else:
                    time_in_queue_histogram.observe(
                        request_output.metrics.time_in_queue
                    )
                    queue_time = f"{request_output.metrics.time_in_queue}s"
                    generation_time = (
                        total_request_time - request_output.metrics.time_in_queue
                    )
                    generation_time_str = f"{generation_time}s"
                    tokens_s = (
                        num_input_tokens + all_tokens_collected
                    ) / generation_time
                    generated_tokens_s = all_tokens_collected / generation_time

                logger.info(
                    f"Request {request.request_id} finished ({finish_reason}). "
                    f"Total time: {total_request_time}s, "
                    f"Queue time: {queue_time}, "
                    f"Generation+async time: {generation_time_str}, "
                    f"Input tokens: {num_input_tokens}, "
                    f"Generated tokens: {all_tokens_collected}, "
                    f"tokens/s: {tokens_s}, "
                    f"generated tokens/s: {generated_tokens_s}."
                )
            else:
                logger.warning(
                    f"Request {request.request_id} "
                    "finished without any output. "
                    f"Input tokens: {num_input_tokens}."
                )
        except ValueError as e:
            error_args = e.args
            if len(error_args) == 3 and "Input too long." == error_args[0]:
                _, input_length, max_input_length = error_args
                raise InputTooLong(input_length, max_input_length).exception from None
            elif len(error_args) == 1 and V1_TOO_LONG_PATTERN.match(error_args[0]):
                parsed_error = V1_TOO_LONG_PATTERN.match(error_args[0])
                raise InputTooLong(
                    int(parsed_error[1]), int(parsed_error[2])
                ).exception from None
            else:
                raise e from None
        finally:
            # Ensure that we cancel on the engine once we have exited the streaming
            # phase
            await self.engine.abort(request.request_id)

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
        self, vllm_embedding_request: VLLMEmbeddingRequest
    ) -> Tuple[List[List[float]], int]:
        """Return (embeddings, num_prompt_tokens)"""

        num_prompts = len(vllm_embedding_request.prompt)
        if RAYLLM_ENABLE_REQUEST_PROMPT_LOGS:
            logger.info(
                f"Encoding request {vllm_embedding_request.request_id} started. "
                f"Num prompts: {num_prompts}"
            )

        generators: List[AsyncGenerator["PoolingRequestOutput", None]] = []

        prompts = vllm_embedding_request.prompt
        if isinstance(prompts, str):
            prompts = [prompts]

        for i, prompt in enumerate(prompts):
            request_id = f"{vllm_embedding_request.request_id}-{i}"
            gen: AsyncGenerator["PoolingRequestOutput", None] = self.engine.encode(
                prompt=vllm.TextPrompt(
                    prompt=prompt,
                ),
                pooling_params=vllm.PoolingParams(),
                request_id=request_id,
                lora_request=vllm_embedding_request.lora_request,  # type: ignore
            )
            generators.append(gen)

        embedding_data = []
        total_prompt_tokens = 0

        for gen in generators:
            async for result in gen:
                if hasattr(result.outputs, "embedding"):
                    embedding = result.outputs.embedding
                else:
                    embedding = result.outputs.data.tolist()
                if vllm_embedding_request.encoding_format == "base64":
                    embedding = floats_to_base64(embedding)

                embedding_data.append(embedding)
                total_prompt_tokens += len(result.prompt_token_ids)

        return embedding_data, total_prompt_tokens

    async def check_health(self) -> None:
        if not hasattr(self.engine, "check_health"):
            raise RuntimeError(f"{type(self.engine)} does not support health check.")

        try:
            await self.engine.check_health()
        except BaseException as e:
            logger.error("Healthcheck failed. The replica will be restarted")
            raise e from None

    @staticmethod
    def _collect_usage_metrics(sampling_params: VLLMSamplingParams) -> None:
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
            usage_counters[ArgUsage.MAX_TOKENS].inc()

        if sampling_params.logprobs is not None:
            usage_counters[ArgUsage.LOGPROBS].inc()

    def _parse_sampling_params(
        self, sampling_params: VLLMSamplingParams
    ) -> "VLLMInternalSamplingParams":
        """Parse the vllm sampling parameters from the prompt.
        This function is used to parse the sampling parameters from the prompt.
        It also collects the usage metrics for the sampling parameters.
        Args:
            sampling_params: The sampling parameters defined in ray.serve.llm.
        Returns:
            vllm.SamplingParams, The parsed sampling parameters.
        """
        self._collect_usage_metrics(sampling_params)
        try:
            if self.model_config is None:
                raise RuntimeError(
                    "VLLMEngine.model_config not set. Maybe VLLMEngine.start() was not called?"
                )

            log_probs = None
            if sampling_params.logprobs:
                max_logprobs = getattr(self.model_config, "max_logprobs", 0)
                max_logprobs = min(MAX_NUM_TOPLOGPROBS_ALLOWED, max_logprobs)
                if max_logprobs == 0:
                    raise ValueError("This model doesn't support outputting logprobs.")
                if sampling_params.top_logprobs:
                    if not (
                        MIN_NUM_TOPLOGPROBS_ALLOWED
                        <= sampling_params.top_logprobs
                        <= max_logprobs
                    ):
                        raise ValueError(
                            f"top_logprobs must be between {MIN_NUM_TOPLOGPROBS_ALLOWED} "
                            f"and {max_logprobs}. Got {sampling_params.top_logprobs}."
                        )
                    log_probs = sampling_params.top_logprobs
                else:
                    log_probs = 1
            else:
                if sampling_params.top_logprobs:
                    raise ValueError(
                        "if top_logprobs is specified, logprobs must be set to `True`"
                    )

            kwargs = dict(
                n=1,
                best_of=sampling_params.best_of,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                repetition_penalty=1.0,
                temperature=1.0,
                top_p=1.0,
                top_k=-1,
                stop=sampling_params.stop,
                stop_token_ids=sampling_params.stop_tokens,
                ignore_eos=False,
                # vLLM will cancel internally if input+output>max_tokens
                max_tokens=self.model_config.max_model_len,
                logprobs=log_probs,
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
                kwargs["max_tokens"] = sampling_params.max_tokens
            # If we set it to None, vLLM will throw an exception
            # as that is not the default value. Omitting it
            # will allow vLLM to generate a new seed internally,
            # as expected.
            if sampling_params.seed is not None:
                kwargs["seed"] = sampling_params.seed
            if sampling_params.response_format is not None:
                kwargs[
                    "guided_decoding"
                ] = sampling_params.response_format.to_guided_decoding_params(
                    backend=RAYLLM_GUIDED_DECODING_BACKEND
                )
            if sampling_params.kv_transfer_params is not None:
                kwargs["extra_args"] = {
                    KV_TRANSFER_PARAMS_KEY: sampling_params.kv_transfer_params
                }

            return vllm.SamplingParams(**kwargs)
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
