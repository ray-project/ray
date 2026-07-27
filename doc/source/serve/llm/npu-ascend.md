# Deploy an LLM on Ascend NPU

This guide provides a step-by-step recipe for deploying DeepSeek-V4-Flash-w8a8-mtp (w8a8 quantized with multi-token prediction) on Huawei Ascend NPUs using [Ray Serve LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html) and [vLLM-Ascend](https://github.com/vllm-project/vllm-ascend), enabling scalable, efficient, and OpenAI-compatible LLM serving on Ascend NPU hardware. If you want to deploy other large language models, you can combine the approach in this guide with the deployment solutions for other models provided in the [vLLM-Ascend documentation](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/index.html).

## Step 1: Download Model Weights

`DeepSeek-V4-Flash-w8a8-mtp` (Quantized version): requires 1 Atlas 800 A3 (128G × 8) node or 1 Atlas 800 A2 (64G × 8) node. [Download model weights](https://www.modelscope.cn/models/Eco-Tech/DeepSeek-V4-Flash-w8a8-mtp)

It is recommended to download the model weights to a shared directory accessible by multiple nodes, such as `/root/.cache/`

## Step 2: Start the Docker Container

This guide demonstrates deployment on an Atlas 800 A2 node. For A3 series deployment, refer to the [vLLM-Ascend DeepSeek-V4-Flash tutorial](https://docs.vllm.ai/projects/ascend/en/v0.18.0/tutorials/models/DeepSeek-V4-Flash.html).

You can use the official Docker image to run `DeepSeek-V4` directly. Adjust the component versions in the image as follows:

| Package | Version |
|---------|---------|
| vllm | 0.22.1 |
| vllm-ascend | 0.22.1 |
| torch | 2.10.0 |
| torch-npu | 2.10.0 |
| cann | 9.0.0 |

```sh
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

## Step 3: Set Environment Variables

Set the following environment variables inside the Docker container:

```sh
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2:$LD_PRELOAD
export HCCL_BUFFSIZE=1024
export VLLM_ASCEND_ENABLE_FLASHCOMM1=1
export TASK_QUEUE_ENABLE=1
export HCCL_OP_EXPANSION_MODE="AIV"
```

## Step 4: Install Ray and Start the Ray Cluster

```sh
pip install ray[llm]
```

> **Note:** The image includes Ray version 2.48.0, which does not support NPU. You need to install the latest version of Ray Serve to enable NPU accelerator type support. Daily builds can be obtained from [Daily Releases](https://docs.ray.io/en/latest/ray-overview/installation.html#daily-releases-nightlies). Before installing, confirm that the version includes NPU support.

Start the Ray cluster and verify that it is running:

```sh
ray start --head
ray status
```

## Step 5: Configure Ray Serve LLM

Create a Python script (e.g., `serve_npu.py`) with the following content. For more details on how to use Ray Serve LLM to deploy LLMs, refer to the [Ray Serve LLM documentation](https://docs.ray.io/en/latest/serve/llm/index.html).

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

```sh
python serve_npu.py
```

## Step 6: Send Requests

You can query the deployed model with cURL:

```sh
curl http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "dsv4",
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
