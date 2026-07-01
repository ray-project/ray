# Deploy an LLM on Ascend NPU

This guide provides a step-by-step recipe for deploying DeepSeek-V4-Flash-w8a8-mtp (w8a8 quantized with multi-token prediction) on Huawei Ascend NPUs using [Ray Serve LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html) and [vLLM-Ascend](https://github.com/vllm-project/vllm-ascend), enabling scalable, efficient, and OpenAI-compatible LLM serving on Ascend NPU hardware. If you want to deploy other large language models, you can combine the approach in this guide with the deployment solutions for other models provided in the [vLLM-Ascend documentation](https://docs.vllm.ai/projects/ascend/en/latest/tutorials/models/index.html).

Ray Serve LLM cannot recognize NPU as an accelerator type. To deploy LLM inference services on Ascend NPU clusters, the latest version of Ray with NPU support is required. However, the latest Ray depends on vllm ≥ 0.22.0, while vllm-ascend's current latest version is 0.21.0, which has not yet been adapted for vllm ≥ 0.22.0. Through testing, vllm 0.22.0 works with vllm-ascend 0.21.0, barely satisfying the new Ray's dependency requirement, but vllm 0.22.0 lacks NPU platform adaptation (already addressed in vllm 0.24.0), requiring manual patching during deployment. The long-term solution is to wait for vllm-ascend to complete adaptation for vllm 0.24.0, enabling a one-time upgrade with no additional patches needed. The immediate deployment plan is described below.

## Step 1: Download Model Weights

`DeepSeek-V4-Flash-w8a8-mtp` (Quantized version): requires 1 Atlas 800 A3 (128G × 8) node or 1 Atlas 800 A2 (64G × 8) node. [Download model weights](https://www.modelscope.cn/models/Eco-Tech/DeepSeek-V4-Flash-w8a8-mtp)

It is recommended to download the model weights to a shared directory accessible by multiple nodes, such as `/root/.cache/`

## Step 2: Start the Docker Container

This guide demonstrates deployment on an Atlas 800 A2 node. For A3 series deployment, refer to the [vLLM-Ascend DeepSeek-V4-Flash tutorial](https://docs.vllm.ai/projects/ascend/en/v0.18.0/tutorials/models/DeepSeek-V4-Flash.html).

You can use the official Docker image to run `DeepSeek-V4` directly. Adjust the component versions in the image as follows:

| Package | Version |
|---------|---------|
| vllm | 0.22.0 |
| vllm-ascend | 0.21.0 |
| torch | 2.10.0 |
| torch-npu | 2.10.0 |
| torchvision | 2.25.0 |
| cann | 9.0.0 |

```sh
export IMAGE=quay.io/ascend/vllm-ascend:v0.21.0rc1
export NAME=vllm-ascend

docker run --rm \
  --name $NAME \
  --net=host \
  --shm-size=512g \
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

## Step 4: Install Ray Serve and Start the Ray Cluster

```sh
pip install "ray[serve]"
ray start --head
```

> **Note:** The image includes Ray version 2.48.0, which does not support NPU. You need to install the latest version of Ray Serve to enable NPU accelerator type support. Daily builds can be obtained from [Daily Releases](https://docs.ray.io/en/latest/ray-overview/installation.html#daily-releases-nightlies). Before installing, confirm that the version includes NPU support.

Verify that the Ray cluster is running:

```sh
ray status
```

## Step 5: Patching

File to modify: vllm/model_executor/layers/rotary_embedding/common.py

Source code:
```python
@CustomOp.register("apply_rotary_emb")
class ApplyRotaryEmb(CustomOp):

    def __init__(
        self,
        enforce_enable: bool = False,
        is_neox_style: bool = True,
        enable_fp32_compute: bool = False,
    ) -> None:
        super().__init__(enforce_enable=enforce_enable)
        self.is_neox_style = is_neox_style
        self.enable_fp32_compute = enable_fp32_compute

        self.apply_rotary_emb_flash_attn = None
        if not current_platform.is_cpu() and find_spec("flash_attn") is not None:
            from flash_attn.ops.triton.rotary import apply_rotary

            self.apply_rotary_emb_flash_attn = apply_rotary
```

Modified code:
```python
@CustomOp.register("apply_rotary_emb")
class ApplyRotaryEmb(CustomOp):

    def __init__(
        self,
        enforce_enable: bool = False,
        is_neox_style: bool = True,
        enable_fp32_compute: bool = False,
    ) -> None:
        super().__init__(enforce_enable=enforce_enable)
        self.is_neox_style = is_neox_style
        self.enable_fp32_compute = enable_fp32_compute

        self.apply_rotary_emb_flash_attn = None
        if not current_platform.is_cpu():
            with suppress(ModuleNotFoundError):
                self.apply_rotary_emb_flash_attn = import_module(
                    "flash_attn.ops.triton.rotary"
                ).apply_rotary
```

## Step 6: Configure Ray Serve LLM

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

## Step 7: Send Requests

You can query the deployed model with cURL:

```sh
curl -X POST http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer fake-key" \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```