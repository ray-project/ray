---
orphan: true
---

# Serve Llama2-7b/70b on a single or multiple Intel Gaudi Accelerator

[Intel Gaudi AI Processors (HPUs)](https://habana.ai) are AI hardware accelerators designed by Intel Habana Labs. See [Gaudi Architecture](https://docs.habana.ai/en/latest/Gaudi_Overview/index.html) and [Gaudi Developer Docs](https://developer.habana.ai/) for more details.

This tutorial has two examples:

1. Deployment of [Llama2-7b](https://huggingface.co/meta-llama/Llama-2-7b-chat-hf) using a single HPU:

    * Load a model onto an HPU.

    * Perform generation on an HPU.

    * Enable HPU Graph optimizations.

2. Deployment of [Llama2-70b](https://huggingface.co/meta-llama/Llama-2-70b-chat-hf) using multiple HPUs on a single node:

    * Initialize a distributed backend.

    * Load a sharded model onto DeepSpeed workers.

    * Stream responses from DeepSpeed workers.

This tutorial serves a large language model (LLM) on HPUs.



## Environment setup

Use a prebuilt container to run these examples. To run a container, you need Docker. See [Install Docker Engine](https://docs.docker.com/engine/install/) for installation instructions.

Next, follow [Run Using Containers](https://docs.habana.ai/en/latest/Installation_Guide/Bare_Metal_Fresh_OS.html?highlight=installer#run-using-containers) to install the Gaudi drivers and container runtime. To verify your installation, start a shell and run `hl-smi`. It should print status information about the HPUs on the machine:

```text
+-----------------------------------------------------------------------------+
| HL-SMI Version:                              hl-1.22.1-fw-61.4.2.1          |
| Driver Version:                                     1.22.1-97ec1a4          |
| Nic Driver Version:                                 1.22.1-97ec1a4          |
|-------------------------------+----------------------+----------------------+
| AIP  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncor-Events|
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | AIP-Util  Compute M. |
|===============================+======================+======================|
|   0  HL-225              N/A  | 0000:33:00.0     N/A |                   2  |
| N/A   22C   P0   88W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   1  HL-225              N/A  | 0000:9a:00.0     N/A |                   2  |
| N/A   22C   P0   83W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   2  HL-225              N/A  | 0000:34:00.0     N/A |                   2  |
| N/A   25C   P0   94W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   3  HL-225              N/A  | 0000:4d:00.0     N/A |                   2  |
| N/A   24C   P0   80W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   4  HL-225              N/A  | 0000:4e:00.0     N/A |                   2  |
| N/A   22C   P0   73W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   5  HL-225              N/A  | 0000:9b:00.0     N/A |                   2  |
| N/A   25C   P0   87W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   6  HL-225              N/A  | 0000:b3:00.0     N/A |                   2  |
| N/A   24C   P0   91W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   7  HL-225              N/A  | 0000:b4:00.0     N/A |                   2  |
| N/A   21C   P0   90W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
| Compute Processes:                                               AIP Memory |
|  AIP       PID   Type   Process name                             Usage      |
|=============================================================================|
|   0        N/A   N/A    N/A                                      N/A        |
|   1        N/A   N/A    N/A                                      N/A        |
|   2        N/A   N/A    N/A                                      N/A        |
|   3        N/A   N/A    N/A                                      N/A        |
|   4        N/A   N/A    N/A                                      N/A        |
|   5        N/A   N/A    N/A                                      N/A        |
|   6        N/A   N/A    N/A                                      N/A        |
|   7        N/A   N/A    N/A                                      N/A        |
+=============================================================================+
```

Next, start the Gaudi container:
```bash
docker pull vault.habana.ai/gaudi-docker/1.22.1/ubuntu24.04/habanalabs/pytorch-installer-2.7.1:latest
docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.22.1/ubuntu24.04/habanalabs/pytorch-installer-2.7.1:latest
```

To follow the examples in this tutorial, mount the directory containing the examples and models into the container. Inside the container, run:
```bash
pip install ray[tune,serve]
pip install git+https://github.com/huggingface/optimum-habana.git
# Replace 1.22.0 with the driver version of the container.
pip install git+https://github.com/HabanaAI/DeepSpeed.git@1.22.0
# Only needed by the DeepSpeed example.
export RAY_EXPERIMENTAL_NOSET_HABANA_VISIBLE_MODULES=1
# Specify your Hugging Face token.
export HF_TOKEN=<YOUR_HF_TOKEN>
```

Start Ray in the container with `ray start --head`. To workaround https://github.com/ray-project/ray/issues/45302 use `ray start --head --resources='{"HPU": 8}'` instead. You are now ready to run the examples.

## Running a model on a single HPU

This example shows how to deploy a Llama2-7b model on an HPU for inference. 

First, define a deployment that serves a Llama2-7b model using an HPU. Note that we enable [HPU graph optimizations](https://docs.habana.ai/en/latest/Gaudi_Overview/SynapseAI_Software_Suite.html?highlight=graph#graph-compiler-and-runtime) for better performance.

```{literalinclude} ../doc_code/intel_gaudi_inference_serve.py
:language: python
:start-after: __model_def_start__
:end-before: __model_def_end__
```

Copy the code above and save it as `intel_gaudi_inference_serve.py`. Start the deployment like this:

```bash
serve run intel_gaudi_inference_serve:entrypoint
```

The terminal should print logs as the deployment starts up:

```text
2025-11-17 17:58:22,030 INFO scripts.py:507 -- Running import path: 'intel_gaudi_inference_serve:entrypoint'.
2025-11-17 17:58:23,132 INFO worker.py:1832 -- Connecting to existing Ray cluster at address: 100.83.67.100:6379...
2025-11-17 17:58:23,144 INFO worker.py:2003 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8265
/usr/local/lib/python3.12/dist-packages/ray/_private/worker.py:2051: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
  warnings.warn(
(ProxyActor pid=1669) INFO 2025-11-17 17:58:26,774 proxy 100.83.67.100 -- Proxy starting on node f3494db6e5cc1aed9325fb7d5a26675ca1a4b06df89852d2b1c6267e (HTTP port: 8000).
INFO 2025-11-17 17:58:26,822 serve 1560 -- Started Serve in namespace "serve".
INFO 2025-11-17 17:58:26,825 serve 1560 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
(ServeController pid=1672) INFO 2025-11-17 17:58:26,905 controller 1672 -- Deploying new version of Deployment(name='LlamaModel', app='default') (initial target replicas: 1).
(ProxyActor pid=1669) INFO 2025-11-17 17:58:26,819 proxy 100.83.67.100 -- Got updated endpoints: {}.
(ProxyActor pid=1669) INFO 2025-11-17 17:58:26,907 proxy 100.83.67.100 -- Got updated endpoints: {Deployment(name='LlamaModel', app='default'): EndpointInfo(route='/', app_is_cross_language=False)}.
(ProxyActor pid=1669) INFO 2025-11-17 17:58:26,911 proxy 100.83.67.100 -- Started <ray.serve._private.router.SharedRouterLongPollClient object at 0x7f9e4731e4e0>.
(ServeController pid=1672) INFO 2025-11-17 17:58:27,007 controller 1672 -- Adding 1 replica to Deployment(name='LlamaModel', app='default').
(ServeReplica:default:LlamaModel pid=1670) Initializing conditional components...
(ServeReplica:default:LlamaModel pid=1670) Using HPU fused kernel for apply_rotary_pos_emb
(ServeReplica:default:LlamaModel pid=1670) Using HPU fused kernel for RMSNorm
(ServeReplica:default:LlamaModel pid=1670) Using HPU fused kernel for apply_rotary_pos_emb
(ServeReplica:default:LlamaModel pid=1670) Using HPU fused kernel for RMSNorm
Fetching 2 files:   0%|          | 0/2 [00:00<?, ?it/s]
Fetching 2 files: 100%|██████████| 2/2 [00:08<00:00,  4.18s/it]
Loading checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s]
Loading checkpoint shards:  50%|█████     | 1/2 [00:01<00:01,  1.36s/it]
Loading checkpoint shards: 100%|██████████| 2/2 [00:01<00:00,  1.07it/s]
(ServeReplica:default:LlamaModel pid=1670) ============================= HPU PT BRIDGE CONFIGURATION ON RANK = 0 =============
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_LAZY_MODE = 1
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_RECIPE_CACHE_CONFIG = ,false,1024,false
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_LAZY_ACC_PAR_MODE = 1
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_EAGER_PIPELINE_ENABLE = 1
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_EAGER_COLLECTIVE_PIPELINE_ENABLE = 1
(ServeReplica:default:LlamaModel pid=1670)  PT_HPU_ENABLE_LAZY_COLLECTIVES = 1
(ServeReplica:default:LlamaModel pid=1670) ---------------------------: System Configuration :---------------------------
(ServeReplica:default:LlamaModel pid=1670) Num CPU Cores : 160
(ServeReplica:default:LlamaModel pid=1670) CPU RAM       : 1007 GB
(ServeReplica:default:LlamaModel pid=1670) ------------------------------------------------------------------------------
INFO 2025-11-17 17:58:55,985 serve 1560 -- Application 'default' is ready at http://127.0.0.1:8000/.

```

In another shell, use the following code to send requests to the deployment to perform generation tasks.

```{literalinclude} ../doc_code/intel_gaudi_inference_client.py
:language: python
:start-after: __main_code_start__
:end-before: __main_code_end__
```

Here is an example output:
```text
Once upon a time, in a small village nestled in the rolling hills of Tuscany, there lived a young girl named Sophia.

Sophia was a curious and adventurous child, always eager to explore the world around her. She spent her days playing in the fields and forests, chasing after butterflies and watching the clouds drift lazily across the sky.

One day, as Sophia was wandering through the village, she stumbled upon a beautiful old book hidden away in a dusty corner of the local library. The book was bound in worn leather and adorned with intr
in a small village nestled in the rolling hills of Tuscany, there lived a young girl named Luna.
Luna was a curious and adventurous child, always eager to explore the world around her. She spent her days wandering through the village, discovering new sights and sounds at every turn.

One day, as she was wandering through the village, Luna stumbled upon a hidden path she had never seen before. The path was overgrown with weeds and vines, and it seemed to disappear into the distance.

Luna's curiosity was piqued,

```

## Running a sharded model on multiple HPUs

This example deploys a Llama2-70b model using 8 HPUs orchestrated by DeepSpeed. 

The example requires caching the Llama2-70b model. Run the following Python code in the Gaudi container to cache the model. 

```python
import os
from huggingface_hub import snapshot_download
snapshot_download(
    "meta-llama/Llama-2-70b-chat-hf",
    # Replace the path if necessary.
    cache_dir=os.getenv("TRANSFORMERS_CACHE", None),
    # Specify your Hugging Face token.
    token=""
)
```

In this example, the deployment replica sends prompts to the DeepSpeed workers, which are running in Ray actors:

```{literalinclude} ../doc_code/intel_gaudi_inference_serve_deepspeed.py
:language: python
:start-after: __worker_def_start__
:end-before: __worker_def_end__
```

Next, define a deployment:

```{literalinclude} ../doc_code/intel_gaudi_inference_serve_deepspeed.py
:language: python
:start-after: __deploy_def_start__
:end-before: __deploy_def_end__
```

Copy both blocks of the preceding code and save them into `intel_gaudi_inference_serve_deepspeed.py`. Run this example using `serve run intel_gaudi_inference_serve_deepspeed:entrypoint`.

Notice!!! Please set the environment variable `HABANA_VISIBLE_MODULES` carefully.

The terminal should print logs as the deployment starts up:
```text
2025-11-17 18:46:18,455 INFO scripts.py:507 -- Running import path: 'intel_gaudi_inference_serve_deepspeed:entrypoint'.
Calling add_step_closure function does not have any effect. It's lazy mode only functionality. (warning logged once)
Calling mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
Calling iter_mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
2025-11-17 18:46:22,135 INFO worker.py:1832 -- Connecting to existing Ray cluster at address: 100.83.67.100:6379...
2025-11-17 18:46:22,146 INFO worker.py:2003 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8265
/usr/local/lib/python3.12/dist-packages/ray/_private/worker.py:2051: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
  warnings.warn(
INFO 2025-11-17 18:46:22,172 serve 19827 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
INFO 2025-11-17 18:46:22,180 serve 19827 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
(ServeController pid=1666) INFO 2025-11-17 18:46:22,269 controller 1666 -- Deploying new version of Deployment(name='DeepSpeedLlamaModel', app='default') (initial target replicas: 1).
(ProxyActor pid=1665) INFO 2025-11-17 18:46:22,272 proxy 100.83.67.100 -- Got updated endpoints: {Deployment(name='DeepSpeedLlamaModel', app='default'): EndpointInfo(route='/', app_is_cross_language=False)}.
(ServeController pid=1666) INFO 2025-11-17 18:46:22,372 controller 1666 -- Removing 1 replica from Deployment(name='LlamaModel', app='default').
(ServeController pid=1666) INFO 2025-11-17 18:46:22,372 controller 1666 -- Adding 1 replica to Deployment(name='DeepSpeedLlamaModel', app='default').
(ServeController pid=1666) INFO 2025-11-17 18:46:24,441 controller 1666 -- Replica(id='fik7t5uv', deployment='LlamaModel', app='default') is stopped.
(ServeReplica:default:DeepSpeedLlamaModel pid=1664) Calling add_step_closure function does not have any effect. It's lazy mode only functionality. (warning logged once)
(ServeReplica:default:DeepSpeedLlamaModel pid=1664) Calling mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
(ServeReplica:default:DeepSpeedLlamaModel pid=1664) Calling iter_mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
(DeepSpeedInferenceWorker pid=20003) Initializing conditional components...
(DeepSpeedInferenceWorker pid=20003) Using HPU fused kernel for apply_rotary_pos_emb
(DeepSpeedInferenceWorker pid=20003) Using HPU fused kernel for RMSNorm
(DeepSpeedInferenceWorker pid=20003) Using HPU fused kernel for apply_rotary_pos_emb
(DeepSpeedInferenceWorker pid=20003) Using HPU fused kernel for RMSNorm
(DeepSpeedInferenceWorker pid=20001) [2025-11-17 18:46:38,172] [INFO] [real_accelerator.py:225:get_accelerator] Setting ds_accelerator to hpu (auto detect)
(DeepSpeedInferenceWorker pid=20001) /usr/local/lib/python3.12/dist-packages/torch/distributed/distributed_c10d.py:4631: UserWarning: No device id is provided via `init_process_group` or `barrier `. Using the current device set by the user.
(DeepSpeedInferenceWorker pid=20001)   warnings.warn(  # warn only once
(pid=20005) Calling add_step_closure function does not have any effect. It's lazy mode only functionality. (warning logged once) [repeated 8x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
(pid=20005) Calling mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once) [repeated 8x across cluster]
(pid=20005) Calling iter_mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once) [repeated 8x across cluster]
Fetching 15 files: 100%|██████████| 15/15 [00:00<00:00, 177724.75it/s]
(DeepSpeedInferenceWorker pid=20003) ============================= HPU PT BRIDGE CONFIGURATION ON RANK = 0 =============
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_LAZY_MODE = 0
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_RECIPE_CACHE_CONFIG = ,false,1024,false
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_LAZY_ACC_PAR_MODE = 0
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_EAGER_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_EAGER_COLLECTIVE_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=20003)  PT_HPU_ENABLE_LAZY_COLLECTIVES = 1
(DeepSpeedInferenceWorker pid=20003) ---------------------------: System Configuration :---------------------------
(DeepSpeedInferenceWorker pid=20003) Num CPU Cores : 160
(DeepSpeedInferenceWorker pid=20003) CPU RAM       : 1007 GB
(DeepSpeedInferenceWorker pid=20003) ------------------------------------------------------------------------------
(DeepSpeedInferenceWorker pid=20003) /usr/local/lib/python3.12/dist-packages/torch/distributed/distributed_c10d.py:4631: UserWarning: No device id is provided via `init_process_group` or `barrier `. Using the current device set by the user.  [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=20003)   warnings.warn(  # warn only once [repeated 7x across cluster]
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s]
(DeepSpeedInferenceWorker pid=20002) [2025-11-17 18:46:44,773] [INFO] [logging.py:107:log_dist] [Rank -1] DeepSpeed info: version=0.16.1+hpu.synapse.v1.22.0, git-hash=9b8097e3, git-branch=1.22.0
(DeepSpeedInferenceWorker pid=20002) [2025-11-17 18:46:44,784] [INFO] [logging.py:107:log_dist] [Rank -1] quantize_bits = 8 mlp_extra_grouping = False, quantize_groups = 1
(DeepSpeedInferenceWorker pid=20002) [2025-11-17 18:46:44,791] [INFO] [comm.py:652:init_distributed] cdb=None
(DeepSpeedInferenceWorker pid=20007) Initializing conditional components... [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=20007) Using HPU fused kernel for apply_rotary_pos_emb [repeated 14x across cluster]
(DeepSpeedInferenceWorker pid=20007) Using HPU fused kernel for RMSNorm [repeated 14x across cluster]
(DeepSpeedInferenceWorker pid=20007) [2025-11-17 18:46:40,521] [INFO] [real_accelerator.py:225:get_accelerator] Setting ds_accelerator to hpu (auto detect) [repeated 7x across cluster]
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:00<00:12,  1.11it/s]
(ServeController pid=1666) WARNING 2025-11-17 18:46:52,471 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Fetching 15 files: 100%|██████████| 15/15 [00:00<00:00, 173318.35it/s] [repeated 8x across cluster]
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s] [repeated 7x across cluster]
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:01<00:14,  1.00s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  13%|█▎        | 2/15 [00:14<01:48,  8.31s/it]
Loading 15 checkpoint shards:  13%|█▎        | 2/15 [00:14<01:50,  8.52s/it]
Loading 15 checkpoint shards:  20%|██        | 3/15 [00:25<01:53,  9.49s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  27%|██▋       | 4/15 [00:35<01:45,  9.55s/it] [repeated 8x across cluster]
(ServeController pid=1666) WARNING 2025-11-17 18:47:22,507 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  33%|███▎      | 5/15 [00:45<01:38,  9.82s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  40%|████      | 6/15 [00:54<01:25,  9.50s/it] [repeated 8x across cluster]
Loading 15 checkpoint shards:  40%|████      | 6/15 [00:59<01:34, 10.48s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  47%|████▋     | 7/15 [01:05<01:20, 10.02s/it] [repeated 2x across cluster]
(ServeController pid=1666) WARNING 2025-11-17 18:47:52,553 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  40%|████      | 6/15 [01:12<01:57, 13.02s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  53%|█████▎    | 8/15 [01:17<01:10, 10.01s/it] [repeated 3x across cluster]
Loading 15 checkpoint shards:  60%|██████    | 9/15 [01:25<01:00, 10.02s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  60%|██████    | 9/15 [01:31<01:03, 10.59s/it] [repeated 5x across cluster]
Loading 15 checkpoint shards:  67%|██████▋   | 10/15 [01:37<00:49, 10.00s/it] [repeated 4x across cluster]
(ServeController pid=1666) WARNING 2025-11-17 18:48:22,586 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  60%|██████    | 9/15 [01:43<01:06, 11.09s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  73%|███████▎  | 11/15 [01:48<00:39,  9.85s/it] [repeated 5x across cluster]
Loading 15 checkpoint shards:  80%|████████  | 12/15 [01:56<00:30, 10.16s/it] [repeated 4x across cluster]
Loading 15 checkpoint shards:  73%|███████▎  | 11/15 [02:01<00:40, 10.23s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [02:07<00:19,  9.67s/it] [repeated 4x across cluster]
(ServeController pid=1666) WARNING 2025-11-17 18:48:52,656 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [02:15<00:09,  9.59s/it]
Loading 15 checkpoint shards:  80%|████████  | 12/15 [02:11<00:30, 10.09s/it] [repeated 4x across cluster]
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [02:19<00:09,  9.71s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [02:20<00:19,  9.83s/it]
Loading 15 checkpoint shards: 100%|██████████| 15/15 [02:26<00:00,  9.73s/it] [repeated 6x across cluster]
(ServeController pid=1666) WARNING 2025-11-17 18:49:22,664 controller 1666 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=1666) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [02:30<00:09,  9.82s/it] [repeated 9x across cluster]
(DeepSpeedInferenceWorker pid=20002) Warning: hpu graphs in eager mode is not supported, ignoring
(DeepSpeedInferenceWorker pid=20005) [2025-11-17 18:46:44,773] [INFO] [logging.py:107:log_dist] [Rank -1] DeepSpeed info: version=0.16.1+hpu.synapse.v1.22.0, git-hash=9b8097e3, git-branch=1.22.0 [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=20005) [2025-11-17 18:46:44,785] [INFO] [logging.py:107:log_dist] [Rank -1] quantize_bits = 8 mlp_extra_grouping = False, quantize_groups = 1 [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=20005) [2025-11-17 18:46:44,792] [INFO] [comm.py:652:init_distributed] cdb=None [repeated 7x across cluster]
INFO 2025-11-17 18:49:25,635 serve 19827 -- Application 'default' is ready at http://127.0.0.1:8000/.
```

Use the same code snippet introduced in the single HPU example to send generation requests. Here's an example output:
```text
Once upon a time, in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.

In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a big smile on her face, ready to face whatever adventures the day might bring.

One day, a wicked sorcerer cast a spell on the kingdom
Once upon a time, in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.

In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a big smile on her face, ready to face whatever adventures the day might bring.

One day, a wicked sorcerer cast a spell on the kingdom
```

## Next Steps
See [llm-on-ray](https://github.com/intel/llm-on-ray) for more ways to customize and deploy LLMs at scale.
