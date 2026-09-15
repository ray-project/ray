---
myst:
  html_meta:
    description: "Deploy DeepSeek-V4-Flash-w8a8-mtp on Huawei Ascend NPUs with Ray Serve LLM and vLLM-Ascend: Docker setup, NPU environment variables, Ray cluster startup, and an OpenAI-compatible endpoint."
---

# Deploy an LLM on Ascend NPU

This guide is a step-by-step recipe for deploying DeepSeek-V4-Flash-w8a8-mtp on Huawei Ascend NPUs with {ref}`Ray Serve LLM <serving-llms>` and [vLLM-Ascend](https://github.com/vllm-project/vllm-ascend). The model is w8a8 quantized with multi-token prediction. To deploy a different model, combine the approach here with the per-model deployment guidance in the [vLLM-Ascend documentation](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/index.html).

## Step 1: Download model weights

`DeepSeek-V4-Flash-w8a8-mtp` requires one Atlas 800 A3 node (128G × 8) or one Atlas 800 A2 node (64G × 8). [Download the model weights](https://www.modelscope.cn/models/Eco-Tech/DeepSeek-V4-Flash-w8a8-mtp).

Download the model weights to a shared directory that all nodes can reach, such as `/root/.cache/`.

## Step 2: Start the Docker container

This guide covers single-node deployment of DeepSeek-V4-Flash-w8a8-mtp on an Atlas 800 A2 node. For A3 series deployment, see the [vLLM-Ascend DeepSeek-V4-Flash tutorial](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/DeepSeek-V4-Flash.html).

You can run `DeepSeek-V4` directly from the official Docker image. For the available versions, see the [vllm-ascend image tags](https://quay.io/repository/ascend/vllm-ascend) on Quay.io. The `docker run` command below targets a single-node Atlas 800 A2, so adjust the `--device` entries and the volume mounts to match your hardware and model. For model-specific guidance, see the [vLLM-Ascend documentation](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/index.html).

Adjust the component versions in the image as follows:

| Package | Version |
|---------|---------|
| vllm | 0.22.1 |
| vllm-ascend | 0.22.1 |
| torch | 2.10.0 |
| torch-npu | 2.10.0 |
| cann | 9.0.0 |

```bash
export IMAGE=quay.io/ascend/vllm-ascend:v0.22.1rc1
docker run --rm \
    --name vllm-ascend \
    --shm-size=512g \
    --net=host \
    --privileged=true \
    --device /dev/davinci0 \
    --device /dev/davinci1 \
    --device /dev/davinci2 \
    --device /dev/davinci3 \
    --device /dev/davinci4 \
    --device /dev/davinci5 \
    --device /dev/davinci6 \
    --device /dev/davinci7 \
    --device /dev/davinci_manager \
    --device /dev/devmm_svm \
    --device /dev/hisi_hdc \
    -v /usr/local/dcmi:/usr/local/dcmi \
    -v /usr/local/Ascend/driver/tools/hccn_tool:/usr/local/Ascend/driver/tools/hccn_tool \
    -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
    -v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/ \
    -v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info \
    -v /etc/ascend_install.info:/etc/ascend_install.info \
    -v /etc/hccn.conf:/etc/hccn.conf \
    -v /root/.cache:/root/.cache \
    -it $IMAGE bash
```

## Step 3: Set environment variables

NPU environment variables are required, but the values vary by model and deployment scenario, such as single-node versus multi-node or prefill-decode disaggregation. For the recommended values for your model, see the [vLLM-Ascend documentation](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/index.html).

The following is an example for DeepSeek-V4-Flash-w8a8-mtp on an Atlas 800 A2 single-node deployment:

```bash
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2:$LD_PRELOAD
export HCCL_BUFFSIZE=1024
export VLLM_ASCEND_ENABLE_FLASHCOMM1=1
export TASK_QUEUE_ENABLE=1
export HCCL_OP_EXPANSION_MODE="AIV"
```

## Step 4: Install Ray and start the Ray cluster

```bash
pip install "ray[llm]"
```

:::{important}
The image includes Ray version 2.48.0, which does not support NPU. You need to install a version of Ray that includes NPU support (NPUConfig, NPUAccelerator).
:::

Start the Ray cluster and verify that it is running:

```bash
ray start --head
ray status
```

## Step 5: Configure Ray Serve LLM

Create a Python script, for example `serve_npu.py`, with the following content. For more on deploying LLMs with Ray Serve LLM, see {ref}`the Ray Serve LLM documentation <serving-llms>`.

```python
from ray.serve.llm import LLMConfig, build_openai_app
from ray import serve

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="deepseek-v4-flash",
        model_source="/root/.cache/DeepSeek-V4-Flash-w8a8-mtp",
    ),
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 1,
        },
    },
    runtime_env={  
        "env_vars": {  
            "VLLM_USE_V1": "1"
        }  
    },
    accelerator_config={"kind":"npu"},
    engine_kwargs=dict(
        tensor_parallel_size=8,
        distributed_executor_backend="ray",
        data_parallel_size=1,
        quantization="ascend",
        enable_expert_parallel=True,
        trust_remote_code=True,
    )
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

Run the deployment:

```bash
python serve_npu.py
```

## Step 6: Send requests

Query the deployed model with `curl`:

```bash
curl http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "deepseek-v4-flash",
        "messages": [
            {
                "role": "user",
                "content": "Who are you?"
            }
        ],
        "max_tokens": 256,
        "temperature": 0
    }'
```
