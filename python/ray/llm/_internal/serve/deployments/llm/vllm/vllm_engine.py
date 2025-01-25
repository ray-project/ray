import asyncio
import os
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, TYPE_CHECKING

import ray
from ray.util import metrics
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.utils import (
    LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
    ClockUnit,
    MsClock,
)
from ray.llm._internal.serve.configs.error_handling import (
    InputTooLong,
    ValidationError,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine_stats import (
    ArgUsage,
    VLLMEngineStats,
    VLLMEngineStatTracker,
    usage_counters,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEngineConfig,
    VLLMGenerationRequest,
    VLLMSamplingParams,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    InitializeNodeOutput,
)
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    initialize_node as initialize_node_util,
)
from ray.llm._internal.serve.configs.server_models import (
    BatchedLLMRawResponse,
    LLMConfig,
    LLMRawResponse,
    LogProb,
    LogProbs,
    FinishReason,
)

from ray.llm._internal.serve.configs.constants import (
    RAYLLM_ENABLE_REQUEST_PROMPT_LOGS,
    RAYLLM_GUIDED_DECODING_BACKEND,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    MIN_NUM_TOPLOGPROBS_ALLOWED,
    MAX_NUM_TOPLOGPROBS_ALLOWED,
)
from ray.llm._internal.utils import try_import

if TYPE_CHECKING:
    from vllm.config import ModelConfig, VllmConfig
    from vllm.engine.arg_utils import AsyncEngineArgs
    from vllm.engine.protocol import EngineClient
    from vllm.outputs import RequestOutput
    from vllm.sampling_params import SamplingParams as VLLMInternalSamplingParams

vllm = try_import("vllm")
logger = get_logger(__name__)


time_in_queue_histogram = metrics.Histogram(
    "vllm_engine_stats_time_in_queue_ms",
    "Time a request spends in the queue first forward pass not included (ms).",
    boundaries=LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS,
)


def _get_async_engine_args(llm_config: LLMConfig) -> "AsyncEngineArgs":
    model = llm_config.model_id
    if isinstance(llm_config.model_loading_config.model_source, str):
        model = llm_config.model_loading_config.model_source

    engine_config = llm_config.get_engine_config()
    return vllm.engine.arg_utils.AsyncEngineArgs(
        **{
            # This is the local path on disk, or the hf model id
            # If it is the hf_model_id, vllm automatically downloads the correct model.
            "model": model,
            "distributed_executor_backend": "ray",
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

    # This check is just to future proof this implementation
    # in case vllm removes their lru_cache decorator
    if hasattr(current_platform.get_device_capability, "cache_clear"):
        logger.info("Clearing the current platform cache ...")
        current_platform.get_device_capability.cache_clear()


class BatchLLMRawResponses:
    """This class batches multiple LLMRawResponses from a generator into a
    single response, at some time interval.

    Args:
        generator: the async generator that this class pulls LLMRawResponses
            from.
        interval_ms: the interval at which this class yields the current batch.
            If None, this class will batch all responses from the generator
            together and yield the entire batch once.
    """

    def __init__(
        self,
        generator: AsyncGenerator[LLMRawResponse, None],
        interval_ms: Optional[float] = MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    ):
        self.generator = generator
        self.queue: asyncio.Queue = asyncio.Queue()

        if interval_ms is None:
            self.interval_s = None
        else:
            self.interval_s = interval_ms / 1000

        self.done_event: asyncio.Event = asyncio.Event()

        # We are okay with this task getting cancelled (to propagate cancellations)
        self.read_task = asyncio.create_task(self.read())

    async def stream(self) -> AsyncGenerator[BatchedLLMRawResponse, None]:
        """Drain from the queue every interval_ms and yield the merged results"""
        try:
            while True:
                # Wait for the interval or until we finish, whichever is faster.
                # We use an event to avoid asyncio.wait_for cancelling the real task on timeout.
                try:
                    if self.interval_s is None:
                        await self.done_event.wait()
                    else:
                        await asyncio.wait_for(
                            self.done_event.wait(), timeout=self.interval_s
                        )
                except asyncio.TimeoutError:
                    pass

                # Get all elements from the queue
                results, is_done = self.check_done_and_drain()

                # If there are results, merge and yield them
                if results:
                    output: BatchedLLMRawResponse = BatchedLLMRawResponse.merge_stream(*results)  # type: ignore
                    yield output

                # If the read task is done, exit the stream task
                if is_done:
                    # Raise exception, if any
                    self.read_task.result()
                    break
        finally:
            # If the stream task is done, make sure to exit the read task
            if not self.read_task.done():
                self.read_task.cancel()

    def check_done_and_drain(self):
        results = self.drain_queue()
        return results, self.read_task.done()

    async def read(self):
        """Read from the generator and put into the queue in a tight loop"""
        try:
            async for x in self.generator:
                self.queue.put_nowait(x)
        finally:
            self.done_event.set()

    def drain_queue(self):
        """Drain all results currently in the queue"""
        results = []
        try:
            while True:
                results.append(self.queue.get_nowait())
        except asyncio.QueueEmpty:
            pass
        return results


class _EngineBackgroundProcess:
    def __init__(self, ipc_path, engine_args, engine_config):
        from vllm.engine.multiprocessing.engine import MQLLMEngine

        # Adapted from vllm.engine.multiprocessing.engine.MQLLMEngine.from_engine_args
        vllm.plugins.load_general_plugins()

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
            usage_context=vllm.usage.usage_lib.UsageContext.API_SERVER,
        )
        self._error = None

    def start(self):
        try:
            self.engine.start()
        except Exception as e:
            self._error = e

    def get_error(self):
        return self._error


class VLLMEngine:
    def __init__(
        self,
        llm_config: LLMConfig,
    ):
        """Create a vLLM Engine class

        Args:
            llm_config: The llm configuration for this engine
        """
        if vllm is None:
            raise ImportError(
                "vLLM is not installed. Please install it with `pip install ray[llm]`."
            )

        assert isinstance(
            llm_config, LLMConfig
        ), f"Got invalid config {llm_config} of type {type(llm_config)}"
        self.llm_config = llm_config
        self.engine_config = VLLMEngineConfig.from_llm_config(llm_config)

        self._stats = VLLMEngineStatTracker()
        self.running = False
        self.model_config: "ModelConfig" = None
        self.engine = None

    @staticmethod
    async def initialize_node(llm_config: LLMConfig) -> InitializeNodeOutput:
        """Run the node initializer.

        This is separate from `start` so it can run concurrently while starting the engine actor.

        It's a static method so it can be overridden for testing.
        """
        return await initialize_node_util(llm_config)

    async def start(self):
        """Start the vLLM engine.

        If the engine is already running, do nothing.
        """
        if self.running:
            # The engine is already running!
            logger.info("Skipping engine restart because the engine is already running")
            return

        # Get the scaling options
        self.engine = await self._start_engine()
        self.running = True
        self.model_config = await self.engine.get_model_config()

        logger.info("Started vLLM engine.")

    async def _start_engine(self) -> "EngineClient":
        from vllm.engine.multiprocessing.client import MQLLMEngineClient

        args: InitializeNodeOutput = await self.initialize_node(self.llm_config)
        engine_args, engine_config = _get_vllm_engine_config(self.llm_config)

        if MQLLMEngineClient.is_unsupported_config(engine_args):
            # If the engine is not supported, we fall back to the legacy async engine.
            #
            # Note (genesu): as of 2025-02-11, this code path is only triggered when
            # pipeline parallelism is > 1. And this is due to the vllm mq engine have
            # not implemented the pipeline parallelism yet.
            return self._start_async_llm_engine(
                engine_args,
                engine_config,
                args.placement_group,
            )

        return await self._start_mq_engine(
            engine_args, engine_config, args.placement_group
        )

    async def _start_mq_engine(
        self,
        engine_args: "AsyncEngineArgs",
        engine_config: "VllmConfig",
        placement_group: PlacementGroup,
    ) -> "EngineClient":
        from vllm.engine.multiprocessing.client import MQLLMEngineClient

        ipc_path = vllm.utils.get_open_zmq_ipc_path()

        BackgroundCls = ray.remote(
            num_cpus=0,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=placement_group,
                placement_group_capture_child_tasks=True,
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
    ) -> "EngineClient":
        """Creates an async LLM engine from the engine arguments."""
        from vllm.executor.ray_distributed_executor import RayDistributedExecutor

        vllm_config.parallel_config.placement_group = placement_group

        _clear_current_platform_cache()

        return vllm.engine.async_llm_engine.AsyncLLMEngine(
            vllm_config=vllm_config,
            executor_class=RayDistributedExecutor,
            log_stats=not engine_args.disable_log_stats,
        )

    async def generate(
        self,
        vllm_engine_request: VLLMGenerationRequest,
        stream: bool,
    ) -> AsyncGenerator[LLMRawResponse, None]:
        batch_interval_ms = MODEL_RESPONSE_BATCH_TIMEOUT_MS if stream else None
        if vllm_engine_request.serve_request_context:
            ray.serve.context._serve_request_context.set(
                vllm_engine_request.serve_request_context
            )
        response_stream = BatchLLMRawResponses(
            self._generate(vllm_engine_request),
            interval_ms=batch_interval_ms,
        )
        async for response in response_stream.stream():
            yield response

    async def _generate(
        self, vllm_generation_request: VLLMGenerationRequest
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
                f"Request {vllm_generation_request.request_id} started. "
                f"Prompt: {vllm_generation_request.prompt}"
            )
        # Construct a results generator from vLLM
        results_generator: AsyncGenerator["RequestOutput", None] = self.engine.generate(
            prompt=vllm.inputs.TextPrompt(
                prompt=vllm_generation_request.prompt,
                multi_modal_data=vllm_generation_request.multi_modal_data,
            ),
            sampling_params=self._parse_sampling_params(
                vllm_generation_request.sampling_params
            ),
            request_id=vllm_generation_request.request_id,
            lora_request=vllm_generation_request.lora_request,  # type: ignore
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
                    vllm_generation_request.sampling_params.top_logprobs,
                )
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
                )

            if request_output is not None:
                time_in_queue_histogram.observe(request_output.metrics.time_in_queue)
                total_request_time = time.perf_counter() - start
                generation_time = (
                    total_request_time - request_output.metrics.time_in_queue
                )
                logger.info(
                    f"Request {vllm_generation_request.request_id} finished ({finish_reason}). "
                    f"Total time: {total_request_time}s, "
                    f"Queue time: {request_output.metrics.time_in_queue}s, "
                    f"Generation+async time: {generation_time}s, "
                    f"Input tokens: {num_input_tokens}, "
                    f"Generated tokens: {all_tokens_collected}, "
                    f"tokens/s: {(num_input_tokens + all_tokens_collected) / generation_time}, "
                    f"generated tokens/s: {all_tokens_collected / generation_time}."
                )
            else:
                logger.warning(
                    f"Request {vllm_generation_request.request_id} "
                    "finished without any output. "
                    f"Input tokens: {num_input_tokens}."
                )
        except ValueError as e:
            error_args = e.args
            if len(error_args) == 3 and "Input too long." == error_args[0]:
                _, input_length, max_input_length = error_args
                raise InputTooLong(input_length, max_input_length).exception from None
            else:
                raise e from None
        finally:
            # Ensure that we cancel on the engine once we have exited the streaming
            # phase
            await self.engine.abort(vllm_generation_request.request_id)

    def _handle_input_too_long(
        self, request_output: "RequestOutput", finish_reason: Optional[FinishReason]
    ):
        if (
            finish_reason
            and finish_reason == FinishReason.LENGTH
            and request_output.metrics.first_token_time is None
        ):
            # This means that the prompt was too long and we did not generate anything.
            raise InputTooLong(
                len(request_output.prompt_token_ids), self.model_config.max_model_len
            ).exception

    async def check_health(self):
        if not hasattr(self.engine, "check_health"):
            return

        try:
            return await asyncio.wait_for(self.engine.check_health(), timeout=15)
        except BaseException as e:
            logger.exception("Healthcheck failed. The replica will be restarted")
            raise e from None

    def stats(self) -> VLLMEngineStats:
        return self._stats.to_stats()

    def shutdown(self, shutdown_pg: bool = True):
        raise NotImplementedError()

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

    @staticmethod
    def _map_response_format_to_extra_fields(
        sampling_params: VLLMSamplingParams,
    ) -> Dict[str, Any]:
        """Map the response format to the extra fields for vLLM."""
        response_format = sampling_params.response_format
        extra_fields = {
            "guided_decoding": response_format.to_guided_decoding_params(
                backend=RAYLLM_GUIDED_DECODING_BACKEND
            )
        }

        return extra_fields

    def _parse_sampling_params(
        self, sampling_params: VLLMSamplingParams, **extra_fields
    ) -> "VLLMInternalSamplingParams":
        # Add vLLM-Anyscale specific fields

        extra_fields = {}
        if sampling_params.response_format is not None:
            extra_fields.update(
                self._map_response_format_to_extra_fields(sampling_params)
            )

        # If we set it to None, vLLM will throw an exception
        # as that is not the default value. Omitting it
        # will allow vLLM to generate a new seed internally,
        # as expected.
        if sampling_params.seed is not None:
            extra_fields["seed"] = sampling_params.seed

        try:
            if sampling_params.n != 1:
                raise ValueError("n>1 is not supported yet in rayllm.")
            self._collect_usage_metrics(sampling_params)
            log_probs = None
            if sampling_params.logprobs:
                # max_log_probs -> anyscale/vllm
                # max_logprobs -> OSS vllm
                max_logprobs = getattr(
                    self.model_config,
                    "max_log_probs",
                    getattr(self.model_config, "max_logprobs", 0),
                )
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

            if self.model_config is None:
                raise RuntimeError(
                    "VLLMEngine.model_config not set. Maybe VLLMEngine.start() was not called?"
                )

            return vllm.sampling_params.SamplingParams(
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
                ignore_eos=False
                if sampling_params.ignore_eos is None
                else sampling_params.ignore_eos,
                # vLLM will cancel internally if input+output>max_tokens
                max_tokens=sampling_params.max_tokens
                or self.model_config.max_model_len,
                logprobs=log_probs,
                **extra_fields,
            )
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
