import asyncio
import pytest
import math
from unittest.mock import MagicMock, AsyncMock, patch

from ray.llm._internal.batch.stages.vllm_engine_stage import (
    vLLMEngineStage,
    vLLMEngineStageUDF,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMEngineWrapper


@pytest.fixture(scope="module")
def dummy_model_ckpt():
    """
    This fixture creates a dummy model checkpoint for testing.
    It uses the facebook/opt-125m model config and tokenizer to generate a dummy model.
    The purpose of this is to avoid downloading the model from HuggingFace hub during
    testing, which is flaky because of the rate limit and HF hub downtime.
    """
    import os
    import tempfile
    import glob
    import zipfile
    from transformers import AutoTokenizer, OPTConfig, OPTForCausalLM

    model_config = {
        "activation_dropout": 0.0,
        "activation_function": "relu",
        "architectures": ["OPTForCausalLM"],
        "attention_dropout": 0.0,
        "bos_token_id": 2,
        "do_layer_norm_before": True,
        "dropout": 0.1,
        "eos_token_id": 2,
        "ffn_dim": 3072,
        "hidden_size": 768,
        "init_std": 0.02,
        "layerdrop": 0.0,
        "max_position_embeddings": 2048,
        "model_type": "opt",
        "num_attention_heads": 12,
        "num_hidden_layers": 12,
        "pad_token_id": 1,
        "prefix": "</s>",
        "torch_dtype": "float16",
        "use_cache": True,
        "vocab_size": 50272,
        "word_embed_proj_dim": 768,
    }

    # Create a dummy model checkpoint.
    config = OPTConfig(**model_config)
    model = OPTForCausalLM(config)

    # Load the tokenizer.
    tokenizer_zip_path = os.path.join(
        os.path.dirname(__file__), "../test_files/opt-tokenizer"
    )
    with tempfile.TemporaryDirectory() as tokenizer_path:
        # Get all .z* files in the directory.
        tokenizer_zip_paths = list(
            glob.glob(os.path.join(tokenizer_zip_path, "opt-tokenizer.zip.*"))
        )
        full_zip_path = os.path.join(tokenizer_path, "opt-tokenizer_merged.zip")
        with open(full_zip_path, "wb") as outfile:
            for zip_path in sorted(tokenizer_zip_paths):
                with open(zip_path, "rb") as infile:
                    outfile.write(infile.read())

        with zipfile.ZipFile(full_zip_path, "r") as zip_ref:
            zip_ref.extractall(tokenizer_path)

        tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)

    # Create a temporary directory and save the model and tokenizer.
    with tempfile.TemporaryDirectory() as checkpoint_dir:

        config.save_pretrained(checkpoint_dir)
        model.save_pretrained(checkpoint_dir)
        tokenizer.save_pretrained(checkpoint_dir)

        yield os.path.abspath(checkpoint_dir)


@pytest.fixture
def mock_vllm_wrapper():
    with patch(
        "ray.llm._internal.batch.stages.vllm_engine_stage.vLLMEngineWrapper"
    ) as mock_wrapper:
        # Create a mock instance that will be returned by the wrapper class
        mock_instance = MagicMock()
        mock_instance.generate_async = AsyncMock()
        mock_instance.shutdown = MagicMock()

        # Configure the mock instance's behavior
        async def mock_generate(row):
            return (
                MagicMock(
                    request_id=0,
                    prompt=row["prompt"],
                    prompt_token_ids=None,
                    images=[],
                    params=row["sampling_params"],
                    idx_in_batch=row["__idx_in_batch"],
                ),
                {
                    "prompt": row["prompt"],
                    "prompt_token_ids": [1, 2, 3],
                    "num_input_tokens": 3,
                    "generated_tokens": [4, 5, 6],
                    "generated_text": f"Response to: {row['prompt']}",
                    "num_generated_tokens": 3,
                    "time_per_token": 0.1,
                },
            )

        mock_instance.generate_async.side_effect = mock_generate

        # Make the wrapper class return our mock instance
        mock_wrapper.return_value = mock_instance
        yield mock_wrapper


def test_vllm_engine_stage_post_init(dummy_model_ckpt):
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model=dummy_model_ckpt,
            engine_kwargs=dict(
                tensor_parallel_size=4,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
            ),
            task_type="generate",
            max_pending_requests=10,
        ),
        map_batches_kwargs=dict(
            zero_copy_batch=True,
            concurrency=1,
            max_concurrency=4,
            accelerator_type="L4",
        ),
    )

    assert stage.fn_constructor_kwargs == {
        "model": dummy_model_ckpt,
        "task_type": "generate",
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tensor_parallel_size": 4,
            "pipeline_parallel_size": 2,
            "distributed_executor_backend": "ray",
        },
    }
    scheduling_strategy = stage.map_batches_kwargs.pop("scheduling_strategy")
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 1,
        "max_concurrency": 4,
        "accelerator_type": "L4",
        "num_gpus": 0,
        "runtime_env": {},
    }
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)

    bundle_specs = scheduling_strategy.placement_group.bundle_specs
    assert len(bundle_specs) == 8
    for bundle_spec in bundle_specs:
        assert bundle_spec["accelerator_type:L4"] == 0.001
        assert bundle_spec["CPU"] == 1.0
        assert bundle_spec["GPU"] == 1.0


@pytest.mark.asyncio
async def test_vllm_engine_udf_basic(mock_vllm_wrapper, dummy_model_ckpt):
    # Create UDF instance - it will use the mocked wrapper
    udf = vLLMEngineStageUDF(
        data_column="__data",
        model=dummy_model_ckpt,
        task_type="generate",
        engine_kwargs={
            # Test that this should be overridden by the stage.
            "model": "random-model",
            # Test that this should be overridden by the stage.
            "task": "random-task",
            "max_num_seqs": 100,
        },
    )

    assert udf.model == dummy_model_ckpt
    assert udf.task_type == "generate"
    assert udf.engine_kwargs["task"] == "generate"
    assert udf.engine_kwargs["max_num_seqs"] == 100
    assert udf.max_pending_requests == math.ceil(100 * 1.1)

    # Test batch processing
    batch = {
        "__data": [
            {"prompt": "Hello", "sampling_params": {"temperature": 0.7}},
            {"prompt": "World", "sampling_params": {"temperature": 0.7}},
        ]
    }

    responses = []
    async for response in udf(batch):
        responses.append(response["__data"][0])

    assert len(responses) == 2
    assert all("batch_uuid" in r for r in responses)
    assert all("time_taken_llm" in r for r in responses)
    # The output order is not guaranteed.
    assert responses[0]["prompt"] in ["Hello", "World"]
    assert responses[1]["prompt"] in ["Hello", "World"]
    assert responses[0]["prompt"] != responses[1]["prompt"]

    # Verify the wrapper was constructed with correct arguments
    mock_vllm_wrapper.assert_called_once_with(
        model=dummy_model_ckpt,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=False,
        max_pending_requests=111,
        runtime_env={},
        task="generate",
        gpu_memory_utilization=0.95,
        use_v2_block_manager=True,
        enable_prefix_caching=False,
        enforce_eager=False,
        pipeline_parallel_size=1,
        max_num_seqs=100,
        tensor_parallel_size=1,
        max_logprobs=0,
    )


@pytest.mark.asyncio
async def test_vllm_wrapper_pending_queue(dummy_model_ckpt):
    from vllm.outputs import RequestOutput, CompletionOutput

    max_pending_requests = 2

    with (
        patch("vllm.AsyncLLMEngine") as mock_engine,
        patch(
            "ray.llm._internal.batch.stages.vllm_engine_stage.vLLMEngineWrapper.generate_async_v0"
        ) as mock_generate_async_v0,
    ):
        mock_engine.from_engine_args.return_value = AsyncMock()
        num_running_requests = 0
        request_lock = asyncio.Lock()

        # Configure mock engine's generate behavior to simulate delay
        async def mock_generate(request):
            nonlocal num_running_requests
            async with request_lock:
                num_running_requests += 1

            assert num_running_requests <= max_pending_requests
            await asyncio.sleep(0.3)

            async with request_lock:
                num_running_requests -= 1

            return RequestOutput(
                request_id="test",
                prompt="test",
                prompt_token_ids=[1, 2, 3],
                prompt_logprobs=None,
                metrics=None,
                outputs=[
                    CompletionOutput(
                        index=0,
                        text="test response",
                        token_ids=[4, 5, 6],
                        cumulative_logprob=None,
                        logprobs=None,
                    )
                ],
                finished=True,
            )

        mock_generate_async_v0.side_effect = mock_generate

        # Create wrapper with max 2 pending requests
        wrapper = vLLMEngineWrapper(
            model=dummy_model_ckpt,
            idx_in_batch_column="__idx_in_batch",
            disable_log_stats=True,
            max_pending_requests=max_pending_requests,
        )

        # Create 10 requests
        batch = [
            {"__idx_in_batch": i, "prompt": f"Test {i}", "sampling_params": {}}
            for i in range(10)
        ]

        tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]
        await asyncio.gather(*tasks)

        # Verify all requests were processed
        assert mock_generate_async_v0.call_count == 10


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v0", "v1"])
async def test_vllm_wrapper_generate(version, dummy_model_ckpt):
    if version == "v1":
        runtime_env = {"env": {"VLLM_USE_V1": "1"}}
    else:
        runtime_env = {}

    wrapper = vLLMEngineWrapper(
        model=dummy_model_ckpt,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        runtime_env=runtime_env,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.3,
        task="generate",
    )

    batch = [
        {
            "__idx_in_batch": 0,
            "prompt": "Hello",
            "sampling_params": {
                "max_tokens": 10,
                "temperature": 0.7,
                "ignore_eos": True,
            },
        },
        {
            "__idx_in_batch": 1,
            "prompt": "World",
            "sampling_params": {
                "max_tokens": 5,
                "temperature": 0.7,
                "ignore_eos": True,
            },
        },
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        request, output = await resp
        params = request.params
        max_tokens = params.max_tokens
        assert max_tokens == output["num_generated_tokens"]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v0", "v1"])
async def test_vllm_wrapper_embed(version, dummy_model_ckpt):
    if version == "v1":
        runtime_env = {"env": {"VLLM_USE_V1": "1"}}
    else:
        runtime_env = {}

    wrapper = vLLMEngineWrapper(
        model=dummy_model_ckpt,
        idx_in_batch_column="__idx_in_batch",
        disable_log_stats=True,
        max_pending_requests=10,
        runtime_env=runtime_env,
        # Skip CUDA graph capturing to reduce the start time.
        enforce_eager=True,
        gpu_memory_utilization=0.3,
        task="embed",
    )

    batch = [
        {"__idx_in_batch": 0, "prompt": "Hello World"},
        {"__idx_in_batch": 1, "prompt": "How are you?"},
    ]

    tasks = [asyncio.create_task(wrapper.generate_async(row)) for row in batch]

    for resp in asyncio.as_completed(tasks):
        _, output = await resp
        assert output["embeddings"].shape == (768,)
