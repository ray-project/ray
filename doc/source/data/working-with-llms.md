---
myst:
  html_meta:
    description: "Batch LLM inference with ray.data.llm: vLLM and SGLang engines or hosted endpoints, multi-GPU scaling, and vision, video, and audio models."
---

(working-with-llms)=

# Working with LLMs

Use the {ref}`ray.data.llm <llm-ref>` module to run scalable batch inference on Ray Data datasets. The module supports two modes. It can run a vLLM or SGLang inference engine directly, or it can query hosted endpoints through {class}`~ray.data.llm.ServeDeploymentProcessorConfig`.

To get started, see the following sections:

- {ref}`Quickstart <vllm_quickstart>`: Run your first batch inference job.
- {ref}`Architecture <processor_architecture>`: Understand the processor pipeline.
- {ref}`Scaling <horizontal_scaling>`: Scale your LLM stage to multiple replicas.

For common use cases, see the following sections:

- {ref}`Text generation <text_generation>`: Generate chat completions with LLMs.
- {ref}`Embeddings <embedding_models>`: Generate text embeddings.
- {ref}`Classification <classification_models>`: Run content classifiers and sentiment analyzers.
- {ref}`Multimodality <multimodal>`: Run batch inference with vision language models (VLMs) and omni-modal models on multimodal data.
- {ref}`OpenAI-compatible endpoints <openai_compatible_api_endpoint>`: Query deployed models.
- {ref}`Serve deployments <serve_deployments>`: Share vLLM engines across processors.
- {ref}`Custom tokenizers <custom_tokenizers>`: Use vLLM tokenizers for models that Hugging Face doesn't support.

To troubleshoot and tune your pipeline, see the following sections:

- {ref}`Troubleshooting <troubleshooting>`: Resolve GPU memory and model loading issues.
- {ref}`Advanced configuration <advanced_configuration>`: Configure parallelism, per-stage tuning, LoRA, and batch concurrency.

(vllm_quickstart)=
(quickstart-vllm-batch-inference)=

## Quickstart: Run batch inference with vLLM

This example shows the minimal setup to run vLLM batch inference on a dataset.

:::{note}
This quickstart requires a GPU because vLLM is GPU-accelerated.
:::

First, install Ray Data with LLM support:

```bash
pip install -U "ray[data, llm]>=2.53.0"
```

The following minimal example runs batch inference:

```{literalinclude} doc_code/working-with-llms/minimal_quickstart.py
:language: python
:start-after: __minimal_vllm_quickstart_start__
:end-before: __minimal_vllm_quickstart_end__
```

This example does the following:

1. Creates a simple dataset with prompts.
1. Configures a vLLM processor with minimal settings.
1. Builds a processor that converts prompts to OpenAI chat format during preprocessing and extracts the generated text during postprocessing.
1. Runs inference on the dataset.
1. Iterates through the results.

The processor expects input rows with a `prompt` field and outputs rows with both `prompt` and `response` fields. Consume the results with `iter_rows()`, `take()`, or `show()`, or save them to files with `write_parquet()`.

For more configuration options and features, see the following sections.

(processor_architecture)=

## How does the processor pipeline work?

Ray Data LLM transforms your data through a *multi-stage processor pipeline* that runs LLM inference. Understanding this architecture helps you optimize performance and debug issues.

```text
Input Dataset
     |
     v
- Preprocess (Custom Function)
- PrepareMultimodal (Optional, for VLM / Omni models)
- ChatTemplate (Applies chat template to messages)
- Tokenize (Optional -- converts text to token IDs)
- LLM Engine (vLLM/SGLang inference on GPU)
- Detokenize (Optional -- converts token IDs back to text)
- Postprocess (Custom Function)
     |
     v
Output Dataset
```

Each stage does the following:

- **Preprocess**: Your custom function that transforms input rows into the format that downstream stages expect, typically OpenAI chat format with `messages`.
- **PrepareMultimodal**: Extracts and prepares multimodal inputs. Enable it with `prepare_multimodal_stage=True`.
- **ChatTemplate**: Applies the model's chat template to convert messages into a prompt string.
- **Tokenize**: Converts the prompt string into token IDs for the model.
- **LLM Engine**: The inference stage that runs vLLM or SGLang on GPU or TPU accelerators.
- **Detokenize**: Converts output token IDs back to readable text.
- **Postprocess**: Your custom function that extracts and formats the final output.

Each stage runs as a separate Ray actor pool, so you can scale and allocate resources for each stage independently. By default, all CPU and GPU stages use autoscaling actor pools, except the ServeDeployment stage, which uses a fixed pool.

(horizontal_scaling)=
(scaling-to-multiple-gpus)=

## Scale to multiple GPUs

To scale the LLM stage horizontally to multiple GPU replicas, set the `concurrency` parameter:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __concurrent_config_example_start__
:end-before: __concurrent_config_example_end__
```

Each replica runs an independent inference engine. Set `concurrency` to match the number of available GPUs or GPU nodes.

By default, when you set `concurrency` to an integer `n`, GPU stages autoscale from 1 to `n` actors. To use a fixed pool of `n` actors, set `concurrency` to `(n, n)`.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __concurrent_config_fixed_pool_example_start__
:end-before: __concurrent_config_fixed_pool_example_end__
```

(text_generation)=

## Generate text

Use {class}`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` or {class}`SGLangEngineProcessorConfig <ray.data.llm.SGLangEngineProcessorConfig>` for chat completions and text generation tasks.

The key configuration options include the following:

- `model_source`: The Hugging Face model ID or the path to the model weights.
- `concurrency`: The number of vLLM engine replicas, typically one per GPU node.
- `batch_size`: The number of rows per batch. Reduce it if you hit memory limits.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __basic_config_example_start__
:end-before: __basic_config_example_end__
```

For gated models that require authentication, pass your Hugging Face token through `runtime_env`:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __hf_token_config_example_start__
:end-before: __hf_token_config_example_end__
```

(multimodal)=

## Run batch inference on multimodal data

Ray Data LLM also supports batch inference with vision language models and omni-modal models on multimodal data. To run multimodal batch inference, make the following two changes to the previous example:

- Set `prepare_multimodal_stage=True` in the `vLLMEngineProcessorConfig`.
- Prepare multimodal data inside the preprocessor.

(image-batch-inference-with-vision-language-model-vlm)=

### Run image batch inference with a vision language model

First, load a vision dataset:

```{literalinclude} doc_code/working-with-llms/vlm_image_example.py
:language: python
:start-after: __vlm_image_load_dataset_example_start__
:end-before: __vlm_image_load_dataset_example_end__
:dedent: 0
```

Next, configure the VLM processor with the essential settings:

```{literalinclude} doc_code/working-with-llms/vlm_image_example.py
:language: python
:start-after: __vlm_config_example_start__
:end-before: __vlm_config_example_end__
```

Define preprocessing and postprocessing functions that convert dataset rows into the format the VLM expects and extract the model responses. In the preprocessor, structure image data as part of an OpenAI-compatible message. You can pass either an image URL or a `PIL.Image.Image` object.

```{literalinclude} doc_code/working-with-llms/vlm_image_example.py
:language: python
:start-after: __image_message_format_example_start__
:end-before: __image_message_format_example_end__
```

```{literalinclude} doc_code/working-with-llms/vlm_image_example.py
:language: python
:start-after: __vlm_preprocess_example_start__
:end-before: __vlm_preprocess_example_end__
```

Finally, run the VLM inference:

```{literalinclude} doc_code/working-with-llms/vlm_image_example.py
:language: python
:start-after: __vlm_run_example_start__
:end-before: __vlm_run_example_end__
:dedent: 0
```

(video-batch-inference-with-vision-language-model-vlm)=

### Run video batch inference with a vision language model

First, load a video dataset:

```{literalinclude} doc_code/working-with-llms/vlm_video_example.py
:language: python
:start-after: __vlm_video_load_dataset_example_start__
:end-before: __vlm_video_load_dataset_example_end__
:dedent: 0
```

Next, configure the VLM processor with the essential settings:

```{literalinclude} doc_code/working-with-llms/vlm_video_example.py
:language: python
:start-after: __vlm_video_config_example_start__
:end-before: __vlm_video_config_example_end__
```

Ray Data LLM forwards `mm_processor_kwargs` to vLLM, which calls the model's Hugging Face processor with those arguments. The Hugging Face processor defines the accepted keys, and they differ by model family. For example, Qwen2-VL accepts `max_pixels` and Qwen3-VL accepts `size`. For the keys your model accepts, see the source of its Hugging Face processor, such as [Qwen3VLVideoProcessor](https://github.com/huggingface/transformers/blob/10555512868d663ee1ff627e4f5c5c260114235b/src/transformers/models/qwen3_vl/video_processing_qwen3_vl.py#L86).

:::{note}
The following arguments configure multimodal inputs:

- `engine_kwargs.limit_mm_per_prompt={"video": 1}`: Caps the number of videos per request.
- `engine_kwargs.mm_processor_kwargs.size`: Sets the per-frame resize budget. The Hugging Face processor resizes inputs to fall between `shortest_edge` and `longest_edge` in total pixels.
- `engine_kwargs.mm_processor_kwargs.do_sample_frames=False`: Skips the Hugging Face processor's own frame sampling because `media_io_kwargs` already produced the final frames. Set this option whenever frame sampling already happened upstream.
- `prepare_multimodal_stage.model_config_kwargs.allowed_local_media_path`: Required for `file://` or local-path media inputs.
- `prepare_multimodal_stage.model_config_kwargs.media_io_kwargs`: Configures frame sampling at decode time.
:::

:::{warning}
If a multimodal input exceeds `mm_processor_kwargs.size`, the Hugging Face processor's [`smart_resize`](https://github.com/huggingface/transformers/blob/10555512868d663ee1ff627e4f5c5c260114235b/src/transformers/models/qwen3_vl/video_processing_qwen3_vl.py#L35) downscales it automatically. Set `size.longest_edge` to match the largest input you expect to process, which is `height * width` for an image and `num_frames * height * width` for a video.
:::

Define preprocessing and postprocessing functions that convert dataset rows into the format the VLM expects and extract the model responses. In the preprocessor, structure video data as part of an OpenAI-compatible message.

```{literalinclude} doc_code/working-with-llms/vlm_video_example.py
:language: python
:start-after: __vlm_video_preprocess_example_start__
:end-before: __vlm_video_preprocess_example_end__
```

Finally, run the VLM inference:

```{literalinclude} doc_code/working-with-llms/vlm_video_example.py
:language: python
:start-after: __vlm_video_run_example_start__
:end-before: __vlm_video_run_example_end__
:dedent: 0
```

(audio-batch-inference-with-omni-modal-model)=

### Run audio batch inference with an omni-modal model

First, load an audio dataset:

```{literalinclude} doc_code/working-with-llms/omni_audio_example.py
:language: python
:start-after: __omni_audio_load_dataset_example_start__
:end-before: __omni_audio_load_dataset_example_end__
:dedent: 0
```

Next, configure the omni-modal processor with the essential settings:

```{literalinclude} doc_code/working-with-llms/omni_audio_example.py
:language: python
:start-after: __omni_audio_config_example_start__
:end-before: __omni_audio_config_example_end__
```

Define preprocessing and postprocessing functions that convert dataset rows into the format the omni-modal model expects and extract the model responses. In the preprocessor, structure audio data as part of an OpenAI-compatible message. You can pass either an audio URL or audio binary data.

```{literalinclude} doc_code/working-with-llms/omni_audio_example.py
:language: python
:start-after: __audio_message_format_example_start__
:end-before: __audio_message_format_example_end__
```

```{literalinclude} doc_code/working-with-llms/omni_audio_example.py
:language: python
:start-after: __omni_audio_preprocess_example_start__
:end-before: __omni_audio_preprocess_example_end__
```

Finally, run the omni-modal inference:

```{literalinclude} doc_code/working-with-llms/omni_audio_example.py
:language: python
:start-after: __omni_audio_run_example_start__
:end-before: __omni_audio_run_example_end__
:dedent: 0
```

(embedding_models)=

## Generate embeddings

For embedding models, set `task_type="embed"` and disable chat templating:

```{literalinclude} doc_code/working-with-llms/embedding_example.py
:language: python
:start-after: __embedding_example_start__
:end-before: __embedding_example_end__
```

Embedding models differ from text generation in the following ways:

- Use `prompt` input instead of `messages`.
- Access results through `row["embeddings"]`.

(classification_models)=

## Run classification models

Ray Data LLM supports batch inference with sequence classification models, such as content classifiers and sentiment analyzers:

```{literalinclude} doc_code/working-with-llms/classification_example.py
:language: python
:start-after: __classification_example_start__
:end-before: __classification_example_end__
```

Classification models differ in the following ways:

- Set `task_type="classify"`, or `task_type="score"` for scoring models.
- Set `chat_template_stage=False` and `detokenize_stage=False`.
- Use direct `prompt` input instead of `messages`.
- Access classification logits through `row["embeddings"]`.

(openai_compatible_api_endpoint)=
(openai-compatible-endpoints)=

## Query OpenAI-compatible endpoints

Query deployed models with an OpenAI-compatible API:

```{literalinclude} doc_code/working-with-llms/openai_api_example.py
:language: python
:start-after: __openai_example_start__
:end-before: __openai_example_end__
```

(tokenization_disaggregation)=

## Configure tokenization disaggregation

By default, tokenization and detokenization run as separate CPU stages in the processor pipeline. This offloads tokenizer work from the GPU stage, so you can scale the CPU and GPU stages independently.

:::{note}
When you enable the detokenize stage, set `detokenize=False` in `sampling_params` so the engine returns raw token IDs for the CPU stage to decode. When you disable it, set `detokenize=True` so the engine decodes the output itself.
:::

The default disaggregated mode runs tokenize and detokenize as separate CPU stages:

```{literalinclude} doc_code/working-with-llms/tokenization_disaggregation_example.py
:language: python
:start-after: __disaggregated_tokenization_start__
:end-before: __disaggregated_tokenization_end__
```

Alternatively, use aggregated mode. Disable these stages so the vLLM engine handles tokenization and detokenization internally:

```{literalinclude} doc_code/working-with-llms/tokenization_aggregation_example.py
:language: python
:start-after: __aggregated_tokenization_start__
:end-before: __aggregated_tokenization_end__
```

:::{tip}
Disaggregated tokenization helps most when the tokenizer is a bottleneck, for example with large vocabularies or long sequences. If the GPU engine is already saturated, the overhead of the extra stages might not pay off.
:::

(custom_tokenizers)=

## Use custom tokenizers

Use this pattern when vLLM supports a model but Hugging Face `transformers` doesn't, for example Mistral Tekken (`mistral`), DeepSeek-V3 (`deepseek_v32`), or Grok-2 (`grok2`). The built-in ChatTemplate, Tokenize, and Detokenize stages rely on Hugging Face and fail for these models. The following example disables the built-in CPU stages and replaces them with `map_batches` callables.

The custom chat template stage converts OpenAI-format messages into the prompt string the model expects. You need this stage because each model family defines its own chat format:

```{literalinclude} doc_code/working-with-llms/custom_tokenizer_example.py
:language: python
:start-after: __custom_chat_template_start__
:end-before: __custom_chat_template_end__
```

The custom tokenize stage converts the prompt string into token IDs for the model:

```{literalinclude} doc_code/working-with-llms/custom_tokenizer_example.py
:language: python
:start-after: __custom_tokenize_start__
:end-before: __custom_tokenize_end__
```

The custom detokenize stage is optional. It decodes generated token IDs back to text. The vLLM engine already returns `generated_text`, so you need this stage only for custom decoding, such as different `skip_special_tokens` settings:

```{literalinclude} doc_code/working-with-llms/custom_tokenizer_example.py
:language: python
:start-after: __custom_detokenize_start__
:end-before: __custom_detokenize_end__
```

Build a processor with the built-in stages disabled and compose the full pipeline:

```{literalinclude} doc_code/working-with-llms/custom_tokenizer_example.py
:language: python
:start-after: __custom_tokenizer_pipeline_start__
:end-before: __custom_tokenizer_pipeline_end__
:dedent: 4
```

:::{note}
This example uses a standard model because the models that require vLLM's custom tokenizer are too large for Ray CI environments. The pattern is the same for those models. Replace `MODEL_ID` and set `tokenizer_mode` explicitly.
:::

(resiliency)=

## How does Ray Data LLM handle failures?

The following sections describe how Ray Data LLM handles row failures and actor failures, and how to resume a pipeline from a checkpoint.

(row-level-fault-tolerance)=

### Continue processing after row failures

To enable row-level fault tolerance, set the `should_continue_on_error` parameter to `True` in the processor config. If a single row fails because of a request-level error from the engine, the job continues processing the remaining rows. Use this option for long-running jobs where you want to minimize the impact of request failures.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __row_level_fault_tolerance_config_example_start__
:end-before: __row_level_fault_tolerance_config_example_end__
```

(actor-level-fault-tolerance)=

### What happens when an actor dies?

When an actor dies during pipeline execution, the actor restarts and rejoins the actor pool to process the remaining rows. This behavior is on by default and needs no extra configuration.

(checkpoint-recovery)=

### Resume from a checkpoint

Ray Data supports checkpoint recovery, so you can resume pipeline execution from a checkpoint stored in local or cloud storage. Checkpointing works only for pipelines that start with a read operation and end with a write operation. For checkpointing to take effect, successful blocks must reach the write sink before a failure occurs. After a failure, resume processing from the checkpoint in a later run.

First, set up the checkpoint configuration and specify the ID column for checkpointing.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __checkpoint_config_setup_example_start__
:end-before: __checkpoint_config_setup_example_end__
```

Then, include a read operation and a write operation in the pipeline to enable checkpoint recovery. Preserve the ID column during postprocessing so the checkpoint stores it.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __checkpoint_usage_example_start__
:end-before: __checkpoint_usage_example_end__
```

To resume from a checkpoint, run the same code again. Ray Data discovers the checkpoint and resumes from the last successful block.

(advanced_configuration)=

## Advanced configuration

The following sections describe options for tuning parallelism, resources, model loading, and throughput.

(model-parallelism)=

### Configure model parallelism

For large models that don't fit on a single GPU, use tensor and pipeline parallelism:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __parallel_config_example_start__
:end-before: __parallel_config_example_end__
```

### Configure cross-node parallelism

Ray Data LLM supports cross-node parallelism, including tensor parallelism and pipeline parallelism. Configure the parallelism level through `engine_kwargs`. The `distributed_executor_backend` defaults to `"ray"` for cross-node support.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __cross_node_parallelism_config_example_start__
:end-before: __cross_node_parallelism_config_example_end__
```

To control how Ray places vLLM engine workers across nodes, customize the placement group configuration. Use `bundle_per_worker` for a basic per-worker resource specification that's replicated automatically based on TP*PP, the tensor parallel size times the pipeline parallel size. Use `bundles` for full control over individual bundles. You can specify the degree of tensor and pipeline parallelism, but the vLLM engine manages the assignment of model ranks to GPUs.

:::{note}
In each bundle dictionary, an omitted `CPU` or `GPU` key counts as `0`. Specify the resources each worker needs explicitly.
:::

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __custom_placement_group_strategy_config_example_start__
:end-before: __custom_placement_group_strategy_config_example_end__
```

(per-stage-configuration)=

### Configure individual stages

Configure each pipeline stage separately for fine-grained resource control:

```python
config = vLLMEngineProcessorConfig(
    model_source="meta-llama/Llama-3.1-8B-Instruct",
    chat_template_stage={
        "enabled": True,
        "batch_size": 256,
        "concurrency": 4,
    },
    tokenize_stage={
        "enabled": True,
        "batch_size": 512,
        "num_cpus": 0.5,
    },
    detokenize_stage={
        "enabled": True,
        "concurrency": (2, 8),  # Autoscaling pool
    },
)
```

See {ref}`stage config classes <stage-configs-ref>` for all available fields.

(lora-adapters)=

### Use LoRA adapters

Configure multi-LoRA batch inference as follows:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __lora_config_example_start__
:end-before: __lora_config_example_end__
```

For details, see {doc}`the vLLM with LoRA example</llm/examples/batch/vllm-with-lora>`.

(accelerated-model-loading-with-runai-streamer)=

### Accelerate model loading with RunAI Model Streamer

Use [RunAI Model Streamer](https://github.com/run-ai/runai-model-streamer) for faster model loading from cloud storage:

:::{note}
To install vLLM with the RunAI dependencies, run `pip install -U "vllm[runai]>=0.10.1"`.
:::

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __runai_config_example_start__
:end-before: __runai_config_example_end__
```

(tuning_concurrent_batches)=
(tuning-concurrent-batch-processing)=

### Tune concurrent batch processing

Two parameters, `max_concurrent_batches` and `max_tasks_in_flight_per_actor`, control concurrent batch processing. Understanding how they interact helps you achieve optimal throughput.

(understanding-the-parameters)=

#### What do the parameters control?

`max_concurrent_batches`, default: 8
: The number of batches that can execute concurrently in a single vLLM engine actor. This overlaps batch processing to hide tail latency. The optimal value depends on the workload.

`max_tasks_in_flight_per_actor`, default: 16
: The number of tasks Ray Data can queue per actor before waiting for results. Ray Data uses this queue to prefetch tasks so they're ready when the actor finishes processing.

(how-they-work-together)=

#### How do the parameters work together?

These parameters control different parts of the pipeline:

- `max_tasks_in_flight_per_actor` controls how many tasks Ray Data sends to the actor queue.
- `max_concurrent_batches` controls how many batches can execute simultaneously.

When `max_tasks_in_flight_per_actor` is less than `max_concurrent_batches`, Ray Data actors aren't fully saturated. To maximize throughput, increase `max_tasks_in_flight_per_actor` to keep the actor task queue saturated.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __concurrent_batches_tuning_example_start__
:end-before: __concurrent_batches_tuning_example_end__
```

(serve_deployments)=

### Share a vLLM engine with Ray Serve

For multi-turn conversations or complex agentic workflows, share a vLLM engine across multiple processors with {ref}`Ray Serve <serving-llms>`:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __shared_vllm_engine_config_example_start__
:end-before: __shared_vllm_engine_config_example_end__
```

---

(troubleshooting)=

## Troubleshooting

The following sections describe common issues and how to resolve them.

### vLLM compatibility

The Ray project fully tests each Ray release with a compatible vLLM version. The following table lists the vLLM version for each release.

```{list-table}
:header-rows: 1
:widths: auto

* - Ray release
  - vLLM version
* - nightly
  - 0.29.0
* - 2.58.0
  - 0.26.0
* - 2.57.0
  - 0.25.1
* - 2.56.0
  - 0.22.0
* - 2.55.0
  - 0.18.0
* - 2.54.0
  - 0.15.0
* - 2.53.0
  - 0.12.0
* - 2.52.0
  - 0.11.0
* - 2.51.0
  - 0.11.0
* - 2.50.0
  - 0.10.2
```

(gpu-memory-and-cuda-oom)=

### Resolve CUDA out-of-memory errors

If you hit CUDA out-of-memory errors, try the following strategies:

- Reduce the batch size. Start with 8 to 16 and increase it gradually.
- Lower `max_num_batched_tokens` from 4096 to 2048 or 1024.
- Decrease `max_model_len` to use shorter context lengths.
- Set `gpu_memory_utilization` to a value between 0.75 and 0.85 instead of the default 0.90.

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __gpu_memory_config_example_start__
:end-before: __gpu_memory_config_example_end__
```

(model-loading-at-scale)=

### Load models at scale

(model_cache)=

On large clusters, Hugging Face might rate-limit model downloads. Cache models to S3 or GCS:

```bash
python -m ray.llm.utils.upload_model \
    --model-source facebook/opt-350m \
    --bucket-uri gs://my-bucket/path/to/model
```

Then reference the remote path in your config:

```{literalinclude} doc_code/working-with-llms/basic_llm_example.py
:language: python
:start-after: __s3_config_example_start__
:end-before: __s3_config_example_end__
```

(vllm-nixl-ep-dependency-incompatibility)=

### Fix the vLLM NIXL EP dependency incompatibility

:::{admonition} Known issue
:class: warning

If you install Ray and vLLM directly, you might see the following NIXL EP incompatibility error:

```text
ImportError: libcudart.so.12: cannot open shared object file: No such file or directory
```

Remove the incompatible package, or make sure the installed `nixl_ep` package is compatible with the CUDA runtime and vLLM build in your environment.
:::

:::{note}
Ray collects anonymous usage data to improve Ray Data LLM. To opt out, see {ref}`usage stats collection <ref-usage-stats>`.
:::

### Get help

If this guide doesn't cover your issue, use the following resources:

- [Ray GitHub Issues](https://github.com/ray-project/ray/issues): Report bugs or request features.
- [Ray Slack](https://ray-distributed.slack.com): Get help from the community.
- [Ray Discourse Forum](https://discuss.ray.io): Ask questions and share knowledge.
- [Ray LLM Office Hours](https://zoom-lfx.platform.linuxfoundation.org/meetings/ray?view=month): Learn about new Ray LLM features, ask questions, and get guidance from the team.
  - [Past Office Hours Recordings](https://youtube.com/playlist?list=PLzTswPQNepXl2IYF8DcV35FdCoVbeL4_6&si=ik81bljIlasYAHKN): View recordings from previous sessions.
