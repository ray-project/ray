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
# The DeepSpeed tag should correspond to the Gaudi driver's major.minor version.
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
2025-11-19 23:07:48,004 INFO scripts.py:507 -- Running import path: 'intel_gaudi_inference_serve_deepspeed:entrypoint'.
Calling add_step_closure function does not have any effect. It's lazy mode only functionality. (warning logged once)
Calling mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
Calling iter_mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
2025-11-19 23:07:51,292 INFO worker.py:1832 -- Connecting to existing Ray cluster at address: 100.83.67.100:6379...
2025-11-19 23:07:51,303 INFO worker.py:2003 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8265
/usr/local/lib/python3.12/dist-packages/ray/_private/worker.py:2051: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
  warnings.warn(
INFO 2025-11-19 23:07:55,175 serve 103800 -- Started Serve in namespace "serve".
INFO 2025-11-19 23:07:55,183 serve 103800 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
(ProxyActor pid=103914) INFO 2025-11-19 23:07:55,127 proxy 100.83.67.100 -- Proxy starting on node f8a9040f72956ae476357ef18a828e58782aac96d5b0c1ab80fcaceb (HTTP port: 8000).
(ProxyActor pid=103914) INFO 2025-11-19 23:07:55,173 proxy 100.83.67.100 -- Got updated endpoints: {}.
(ServeController pid=103911) INFO 2025-11-19 23:07:55,257 controller 103911 -- Deploying new version of Deployment(name='DeepSpeedLlamaModel', app='default') (initial target replicas: 1).
(ProxyActor pid=103914) INFO 2025-11-19 23:07:55,259 proxy 100.83.67.100 -- Got updated endpoints: {Deployment(name='DeepSpeedLlamaModel', app='default'): EndpointInfo(route='/', app_is_cross_language=False)}.
(ProxyActor pid=103914) INFO 2025-11-19 23:07:55,264 proxy 100.83.67.100 -- Started <ray.serve._private.router.SharedRouterLongPollClient object at 0x7f8c55d635f0>.
(ServeController pid=103911) INFO 2025-11-19 23:07:55,362 controller 103911 -- Adding 1 replica to Deployment(name='DeepSpeedLlamaModel', app='default').
(ServeReplica:default:DeepSpeedLlamaModel pid=103912) Calling add_step_closure function does not have any effect. It's lazy mode only functionality. (warning logged once)
(ServeReplica:default:DeepSpeedLlamaModel pid=103912) Calling mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
(ServeReplica:default:DeepSpeedLlamaModel pid=103912) Calling iter_mark_step function does not have any effect. It's lazy mode only functionality. (warning logged once)
(DeepSpeedInferenceWorker pid=119844) Initializing conditional components...
(DeepSpeedInferenceWorker pid=119844) Using HPU fused kernel for apply_rotary_pos_emb
(DeepSpeedInferenceWorker pid=119844) Using HPU fused kernel for RMSNorm
(DeepSpeedInferenceWorker pid=119844) Using HPU fused kernel for apply_rotary_pos_emb
(DeepSpeedInferenceWorker pid=119844) Using HPU fused kernel for RMSNorm
(DeepSpeedInferenceWorker pid=119845) [2025-11-19 23:08:10,967] [INFO] [real_accelerator.py:225:get_accelerator] Setting ds_accelerator to hpu (auto detect)
(DeepSpeedInferenceWorker pid=119845) /usr/local/lib/python3.12/dist-packages/torch/distributed/distributed_c10d.py:4631: UserWarning: No device id is provided via `init_process_group` or `barrier `. Using the current device set by the user.
(DeepSpeedInferenceWorker pid=119845)   warnings.warn(  # warn only once
Fetching 15 files: 100%|██████████| 15/15 [00:00<00:00, 177724.75it/s]
(DeepSpeedInferenceWorker pid=119841) ============================= HPU PT BRIDGE CONFIGURATION ON RANK = 0 =============
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_LAZY_MODE = 1
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_RECIPE_CACHE_CONFIG = ,false,1024,false
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_LAZY_ACC_PAR_MODE = 0
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_EAGER_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_EAGER_COLLECTIVE_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=119841)  PT_HPU_ENABLE_LAZY_COLLECTIVES = 1
(DeepSpeedInferenceWorker pid=119841) ---------------------------: System Configuration :---------------------------
(DeepSpeedInferenceWorker pid=119841) Num CPU Cores : 160
(DeepSpeedInferenceWorker pid=119841) CPU RAM       : 1007 GB
(DeepSpeedInferenceWorker pid=119841) ------------------------------------------------------------------------------
(DeepSpeedInferenceWorker pid=119841) /usr/local/lib/python3.12/dist-packages/torch/distributed/distributed_c10d.py:4631: UserWarning: No device id is provided via `init_process_group` or `barrier `. Using the current device set by the user.  [repeated 7x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
(DeepSpeedInferenceWorker pid=119841)   warnings.warn(  # warn only once [repeated 7x across cluster]
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s]
(DeepSpeedInferenceWorker pid=119841) [2025-11-19 23:08:17,597] [INFO] [logging.py:107:log_dist] [Rank -1] DeepSpeed info: version=0.16.1+hpu.synapse.v1.22.0, git-hash=9b8097e3, git-branch=1.22.0
(DeepSpeedInferenceWorker pid=119841) [2025-11-19 23:08:17,601] [INFO] [logging.py:107:log_dist] [Rank -1] quantize_bits = 8 mlp_extra_grouping = False, quantize_groups = 1
(DeepSpeedInferenceWorker pid=119841) [2025-11-19 23:08:17,630] [INFO] [comm.py:652:init_distributed] cdb=None
(DeepSpeedInferenceWorker pid=119840) Initializing conditional components... [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=119840) Using HPU fused kernel for apply_rotary_pos_emb [repeated 14x across cluster]
(DeepSpeedInferenceWorker pid=119840) Using HPU fused kernel for RMSNorm [repeated 14x across cluster]
(DeepSpeedInferenceWorker pid=119840) [2025-11-19 23:08:13,003] [INFO] [real_accelerator.py:225:get_accelerator] Setting ds_accelerator to hpu (auto detect) [repeated 7x across cluster]
(ServeController pid=103911) WARNING 2025-11-19 23:08:25,428 controller 103911 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=103911) This may be caused by a slow __init__ or reconfigure method.
Fetching 15 files: 100%|██████████| 15/15 [00:00<00:00, 174278.56it/s] [repeated 8x across cluster]
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s] [repeated 7x across cluster]
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:08<01:55,  8.27s/it]
Loading 15 checkpoint shards:  13%|█▎        | 2/15 [00:16<01:45,  8.12s/it] [repeated 8x across cluster]
Loading 15 checkpoint shards:  20%|██        | 3/15 [00:24<01:36,  8.07s/it] [repeated 8x across cluster]
Loading 15 checkpoint shards:  27%|██▋       | 4/15 [00:32<01:29,  8.17s/it] [repeated 8x across cluster]
(ServeController pid=103911) WARNING 2025-11-19 23:08:55,478 controller 103911 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=103911) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  27%|██▋       | 4/15 [00:35<01:38,  8.97s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  33%|███▎      | 5/15 [00:45<01:32,  9.28s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  40%|████      | 6/15 [00:50<01:16,  8.45s/it] [repeated 4x across cluster]
Loading 15 checkpoint shards:  47%|████▋     | 7/15 [00:57<01:05,  8.20s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  47%|████▋     | 7/15 [01:02<01:10,  8.84s/it] [repeated 6x across cluster]
(ServeController pid=103911) WARNING 2025-11-19 23:09:25,503 controller 103911 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=103911) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  53%|█████▎    | 8/15 [01:07<00:58,  8.40s/it] [repeated 6x across cluster]
Loading 15 checkpoint shards:  53%|█████▎    | 8/15 [01:13<01:04,  9.28s/it] [repeated 3x across cluster]
Loading 15 checkpoint shards:  60%|██████    | 9/15 [01:19<00:51,  8.54s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  67%|██████▋   | 10/15 [01:27<00:42,  8.41s/it] [repeated 8x across cluster]
Loading 15 checkpoint shards:  73%|███████▎  | 11/15 [01:35<00:33,  8.34s/it] [repeated 8x across cluster]
(ServeController pid=103911) WARNING 2025-11-19 23:09:55,564 controller 103911 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=103911) This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  80%|████████  | 12/15 [01:43<00:25,  8.38s/it] [repeated 8x across cluster]
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [01:46<00:05,  5.81s/it]
Loading 15 checkpoint shards:  80%|████████  | 12/15 [01:48<00:26,  8.95s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [01:49<00:06,  6.04s/it] [repeated 5x across cluster]
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [01:52<00:16,  8.34s/it]
Loading 15 checkpoint shards: 100%|██████████| 15/15 [01:57<00:00,  7.83s/it] [repeated 11x across cluster]
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [01:57<00:18,  9.06s/it]
Loading 15 checkpoint shards: 100%|██████████| 15/15 [02:07<00:00,  7.21s/it] [repeated 6x across cluster]
(ServeController pid=103911) WARNING 2025-11-19 23:10:25,632 controller 103911 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=103911) This may be caused by a slow __init__ or reconfigure method.
INFO 2025-11-19 23:10:26,558 serve 103800 -- Application 'default' is ready at http://127.0.0.1:8000/.
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
