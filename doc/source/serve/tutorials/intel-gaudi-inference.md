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
| HL-SMI Version:                              hl-1.20.0-fw-58.1.1.1          |
| Driver Version:                                     1.19.1-6f47ddd          |
| Nic Driver Version:                                 1.19.1-f071c23          |
|-------------------------------+----------------------+----------------------+
| AIP  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncor-Events|
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | AIP-Util  Compute M. |
|===============================+======================+======================|
|   0  HL-225              N/A  | 0000:9a:00.0     N/A |                   0  |
| N/A   22C   N/A  96W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   1  HL-225              N/A  | 0000:9b:00.0     N/A |                   0  |
| N/A   24C   N/A  78W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   2  HL-225              N/A  | 0000:b3:00.0     N/A |                   0  |
| N/A   25C   N/A  81W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   3  HL-225              N/A  | 0000:b4:00.0     N/A |                   0  |
| N/A   22C   N/A  92W /  600W  | 96565MiB /  98304MiB |     0%           98% |
|-------------------------------+----------------------+----------------------+
|   4  HL-225              N/A  | 0000:33:00.0     N/A |                   0  |
| N/A   22C   N/A  83W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   5  HL-225              N/A  | 0000:4e:00.0     N/A |                   0  |
| N/A   21C   N/A  80W /  600W  | 96564MiB /  98304MiB |     0%           98% |
|-------------------------------+----------------------+----------------------+
|   6  HL-225              N/A  | 0000:34:00.0     N/A |                   0  |
| N/A   25C   N/A  86W /  600W  |   768MiB /  98304MiB |     0%            0% |
|-------------------------------+----------------------+----------------------+
|   7  HL-225              N/A  | 0000:4d:00.0     N/A |                   0  |
| N/A   30C   N/A 100W /  600W  | 17538MiB /  98304MiB |     0%           17% |
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
|   7       107684     C   ray::_RayTrainW                         16770MiB    
+=============================================================================+
```

Next, start the Gaudi container:
```bash
docker pull vault.habana.ai/gaudi-docker/1.20.0/ubuntu22.04/habanalabs/pytorch-installer-2.6.0:latest
docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.20.0/ubuntu22.04/habanalabs/pytorch-installer-2.6.0:latest
```

To follow the examples in this tutorial, mount the directory containing the examples and models into the container. Inside the container, run:
```bash
pip install ray[tune,serve]
pip install git+https://github.com/huggingface/optimum-habana.git
# Replace 1.20.0 with the driver version of the container.
pip install git+https://github.com/HabanaAI/DeepSpeed.git@1.20.0
# Only needed by the DeepSpeed example.
export RAY_EXPERIMENTAL_NOSET_HABANA_VISIBLE_MODULES=1
```

Start Ray in the container with `ray start --head`. You are now ready to run the examples.

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
2025-03-03 06:07:08,106 INFO scripts.py:494 -- Running import path: 'infer:entrypoint'.
2025-03-03 06:07:09,295 INFO worker.py:1654 -- Connecting to existing Ray cluster at address: 100.83.111.228:6379...
2025-03-03 06:07:09,304 INFO worker.py:1832 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265 
(ProxyActor pid=147082) INFO 2025-03-03 06:07:11,096 proxy 100.83.111.228 -- Proxy starting on node b4d028b67678bfdd190b503b44780bc319c07b1df13ac5c577873861 (HTTP port: 8000).
INFO 2025-03-03 06:07:11,202 serve 162730 -- Started Serve in namespace "serve".
INFO 2025-03-03 06:07:11,203 serve 162730 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
(ProxyActor pid=147082) INFO 2025-03-03 06:07:11,184 proxy 100.83.111.228 -- Got updated endpoints: {}.
(ServeController pid=147087) INFO 2025-03-03 06:07:11,278 controller 147087 -- Deploying new version of Deployment(name='LlamaModel', app='default') (initial target replicas: 1).
(ProxyActor pid=147082) INFO 2025-03-03 06:07:11,280 proxy 100.83.111.228 -- Got updated endpoints: {Deployment(name='LlamaModel', app='default'): EndpointInfo(route='/', app_is_cross_language=False)}.
(ProxyActor pid=147082) INFO 2025-03-03 06:07:11,286 proxy 100.83.111.228 -- Started <ray.serve._private.router.SharedRouterLongPollClient object at 0x7f74804e90c0>.
(ServeController pid=147087) INFO 2025-03-03 06:07:11,381 controller 147087 -- Adding 1 replica to Deployment(name='LlamaModel', app='default').
(ServeReplica:default:LlamaModel pid=147085) [WARNING|utils.py:212] 2025-03-03 06:07:15,251 >> optimum-habana v1.15.0 has been validated for SynapseAI v1.19.0 but habana-frameworks v1.20.0.543 was found, this could lead to undefined behavior!
(ServeReplica:default:LlamaModel pid=147085) /usr/local/lib/python3.10/dist-packages/transformers/deepspeed.py:24: FutureWarning: transformers.deepspeed module is deprecated and will be removed in a future version. Please import deepspeed modules directly from transformers.integrations
(ServeReplica:default:LlamaModel pid=147085)   warnings.warn(
(ServeReplica:default:LlamaModel pid=147085) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/tokenization_auto.py:796: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers. Please use `token` instead.
(ServeReplica:default:LlamaModel pid=147085)   warnings.warn(
(ServeReplica:default:LlamaModel pid=147085) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/configuration_auto.py:991: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers. Please use `token` instead.
(ServeReplica:default:LlamaModel pid=147085)   warnings.warn(
(ServeReplica:default:LlamaModel pid=147085) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/auto_factory.py:471: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers. Please use `token` instead.
(ServeReplica:default:LlamaModel pid=147085)   warnings.warn(
Loading checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s]
Loading checkpoint shards:  50%|█████     | 1/2 [00:01<00:01,  1.72s/it]
Loading checkpoint shards: 100%|██████████| 2/2 [00:02<00:00,  1.45s/it]
(ServeReplica:default:LlamaModel pid=147085) ============================= HABANA PT BRIDGE CONFIGURATION =========================== 
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_LAZY_MODE = 1
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_RECIPE_CACHE_CONFIG = ,false,1024
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_LAZY_ACC_PAR_MODE = 1
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_EAGER_PIPELINE_ENABLE = 1
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_EAGER_COLLECTIVE_PIPELINE_ENABLE = 1
(ServeReplica:default:LlamaModel pid=147085)  PT_HPU_ENABLE_LAZY_COLLECTIVES = 0
(ServeReplica:default:LlamaModel pid=147085) ---------------------------: System Configuration :---------------------------
(ServeReplica:default:LlamaModel pid=147085) Num CPU Cores : 160
(ServeReplica:default:LlamaModel pid=147085) CPU RAM       : 1056374420 KB
(ServeReplica:default:LlamaModel pid=147085) ------------------------------------------------------------------------------
INFO 2025-03-03 06:07:30,359 serve 162730 -- Application 'default' is ready at http://127.0.0.1:8000/.
INFO 2025-03-03 06:07:30,359 serve 162730 -- Deployed app 'default' successfully.

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
2025-03-03 06:21:57,692 INFO scripts.py:494 -- Running import path: 'infer-ds:entrypoint'.
2025-03-03 06:22:03,064 INFO worker.py:1832 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
INFO 2025-03-03 06:22:07,343 serve 170212 -- Started Serve in namespace "serve".
INFO 2025-03-03 06:22:07,343 serve 170212 -- Connecting to existing Serve app in namespace "serve". New http options will not be applied.
(ServeController pid=170719) INFO 2025-03-03 06:22:07,377 controller 170719 -- Deploying new version of Deployment(name='DeepSpeedLlamaModel', app='default') (initial target replicas: 1).
(ProxyActor pid=170723) INFO 2025-03-03 06:22:07,290 proxy 100.83.111.228 -- Proxy starting on node 47721c925467a877497e66104328bb72dc7bd7f900a63b2f1fdb48b2 (HTTP port: 8000).
(ProxyActor pid=170723) INFO 2025-03-03 06:22:07,325 proxy 100.83.111.228 -- Got updated endpoints: {}.
(ProxyActor pid=170723) INFO 2025-03-03 06:22:07,379 proxy 100.83.111.228 -- Got updated endpoints: {Deployment(name='DeepSpeedLlamaModel', app='default'): EndpointInfo(route='/', app_is_cross_language=False)}.
(ServeController pid=170719) INFO 2025-03-03 06:22:07,478 controller 170719 -- Adding 1 replica to Deployment(name='DeepSpeedLlamaModel', app='default').
(ProxyActor pid=170723) INFO 2025-03-03 06:22:07,422 proxy 100.83.111.228 -- Started <ray.serve._private.router.SharedRouterLongPollClient object at 0x7fa557945210>.
(DeepSpeedInferenceWorker pid=179962) [WARNING|utils.py:212] 2025-03-03 06:22:14,611 >> optimum-habana v1.15.0 has been validated for SynapseAI v1.19.0 but habana-frameworks v1.20.0.543 was found, this could lead to undefined behavior!
(DeepSpeedInferenceWorker pid=179963) /usr/local/lib/python3.10/dist-packages/transformers/deepspeed.py:24: FutureWarning: transformers.deepspeed module is deprecated and will be removed in a future version. Please import deepspeed modules directly from transformers.integrations
(DeepSpeedInferenceWorker pid=179963)   warnings.warn(
(DeepSpeedInferenceWorker pid=179964) [WARNING|utils.py:212] 2025-03-03 06:22:14,613 >> optimum-habana v1.15.0 has been validated for SynapseAI v1.19.0 but habana-frameworks v1.20.0.543 was found, this could lead to undefined behavior! [repeated 3x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
(DeepSpeedInferenceWorker pid=179962) [2025-03-03 06:22:23,502] [INFO] [real_accelerator.py:219:get_accelerator] Setting ds_accelerator to hpu (auto detect)
Loading 2 checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s]
(DeepSpeedInferenceWorker pid=179962) [2025-03-03 06:22:24,032] [INFO] [logging.py:105:log_dist] [Rank -1] DeepSpeed info: version=0.16.1+hpu.synapse.v1.20.0, git-hash=61543a96, git-branch=1.20.0
(DeepSpeedInferenceWorker pid=179962) [2025-03-03 06:22:24,035] [INFO] [logging.py:105:log_dist] [Rank -1] quantize_bits = 8 mlp_extra_grouping = False, quantize_groups = 1
(DeepSpeedInferenceWorker pid=179962) [2025-03-03 06:22:24,048] [INFO] [comm.py:652:init_distributed] cdb=None
(DeepSpeedInferenceWorker pid=179963) ============================= HABANA PT BRIDGE CONFIGURATION =========================== 
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_LAZY_MODE = 1
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_RECIPE_CACHE_CONFIG = ,false,1024
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_LAZY_ACC_PAR_MODE = 0
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_EAGER_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_EAGER_COLLECTIVE_PIPELINE_ENABLE = 1
(DeepSpeedInferenceWorker pid=179963)  PT_HPU_ENABLE_LAZY_COLLECTIVES = 1
(DeepSpeedInferenceWorker pid=179963) ---------------------------: System Configuration :---------------------------
(DeepSpeedInferenceWorker pid=179963) Num CPU Cores : 160
(DeepSpeedInferenceWorker pid=179963) CPU RAM       : 1056374420 KB
(DeepSpeedInferenceWorker pid=179963) ------------------------------------------------------------------------------
(DeepSpeedInferenceWorker pid=179964) /usr/local/lib/python3.10/dist-packages/transformers/deepspeed.py:24: FutureWarning: transformers.deepspeed module is deprecated and will be removed in a future version. Please import deepspeed modules directly from transformers.integrations [repeated 3x across cluster]
(DeepSpeedInferenceWorker pid=179964)   warnings.warn( [repeated 3x across cluster]
Loading 2 checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s] [repeated 3x across cluster]
(ServeController pid=170719) WARNING 2025-03-03 06:22:37,562 controller 170719 -- Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize.
(ServeController pid=170719) This may be caused by a slow __init__ or reconfigure method.
Loading 2 checkpoint shards:  50%|█████     | 1/2 [00:17<00:17, 17.51s/it]
Loading 2 checkpoint shards: 100%|██████████| 2/2 [00:21<00:00,  9.57s/it]
Loading 2 checkpoint shards: 100%|██████████| 2/2 [00:21<00:00, 10.88s/it]
Loading 2 checkpoint shards:  50%|█████     | 1/2 [00:18<00:18, 18.70s/it] [repeated 3x across cluster]
INFO 2025-03-03 06:22:48,569 serve 170212 -- Application 'default' is ready at http://127.0.0.1:8000/.
INFO 2025-03-03 06:22:48,569 serve 170212 -- Deployed app 'default' successfully.
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
