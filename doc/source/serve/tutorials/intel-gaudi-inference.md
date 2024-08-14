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
| HL-SMI Version:                              hl-1.14.0-fw-48.0.1.0          |
| Driver Version:                                     1.15.0-c43dc7b          |
|-------------------------------+----------------------+----------------------+
| AIP  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | AIP-Util  Compute M. |
|===============================+======================+======================|
|   0  HL-225              N/A  | 0000:09:00.0     N/A |                   0  |
| N/A   26C   N/A    87W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   1  HL-225              N/A  | 0000:08:00.0     N/A |                   0  |
| N/A   28C   N/A    99W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   2  HL-225              N/A  | 0000:0a:00.0     N/A |                   0  |
| N/A   24C   N/A    98W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   3  HL-225              N/A  | 0000:0c:00.0     N/A |                   0  |
| N/A   27C   N/A    87W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   4  HL-225              N/A  | 0000:0b:00.0     N/A |                   0  |
| N/A   25C   N/A   112W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   5  HL-225              N/A  | 0000:0d:00.0     N/A |                   0  |
| N/A   26C   N/A   111W / 600W |  26835MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   6  HL-225              N/A  | 0000:0f:00.0     N/A |                   0  |
| N/A   24C   N/A    93W / 600W |    768MiB / 98304MiB |     0%           N/A |
|-------------------------------+----------------------+----------------------+
|   7  HL-225              N/A  | 0000:0e:00.0     N/A |                   0  |
| N/A   25C   N/A    86W / 600W |    768MiB / 98304MiB |     0%           N/A |
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
docker pull vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
```

To follow the examples in this tutorial, mount the directory containing the examples and models into the container. Inside the container, run:
```bash
pip install ray[tune,serve]
pip install git+https://github.com/huggingface/optimum-habana.git
# Replace 1.14.0 with the driver version of the container.
pip install git+https://github.com/HabanaAI/DeepSpeed.git@1.14.0
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
2024-02-01 05:38:34,021 INFO scripts.py:438 -- Running import path: 'ray_serve_7b:entrypoint'.
2024-02-01 05:38:36,112 INFO worker.py:1540 -- Connecting to existing Ray cluster at address: 10.111.128.177:6379...
2024-02-01 05:38:36,124 INFO worker.py:1715 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265 
(ProxyActor pid=17179) INFO 2024-02-01 05:38:39,573 proxy 10.111.128.177 proxy.py:1141 - Proxy actor b0c697edb66f42a46f802f4603000000 starting on node 7776cd4634f69216c8354355018195b290314ad24fd9565404a2ed12.
(ProxyActor pid=17179) INFO 2024-02-01 05:38:39,580 proxy 10.111.128.177 proxy.py:1346 - Starting HTTP server on node: 7776cd4634f69216c8354355018195b290314ad24fd9565404a2ed12 listening on port 8000
(ProxyActor pid=17179) INFO:     Started server process [17179]
(ServeController pid=17084) INFO 2024-02-01 05:38:39,677 controller 17084 deployment_state.py:1545 - Deploying new version of deployment LlamaModel in application 'default'. Setting initial target number of replicas to 1.
(ServeController pid=17084) INFO 2024-02-01 05:38:39,780 controller 17084 deployment_state.py:1829 - Adding 1 replica to deployment LlamaModel in application 'default'.
(ServeReplica:default:LlamaModel pid=17272) [WARNING|utils.py:198] 2024-02-01 05:38:48,700 >> optimum-habana v1.11.0.dev0 has been validated for SynapseAI v1.14.0 but the driver version is v1.15.0, this could lead to undefined behavior!
(ServeReplica:default:LlamaModel pid=17272) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/tokenization_auto.py:655: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers.
(ServeReplica:default:LlamaModel pid=17272)   warnings.warn(
(ServeReplica:default:LlamaModel pid=17272) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/configuration_auto.py:1020: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers.
(ServeReplica:default:LlamaModel pid=17272)   warnings.warn(
(ServeReplica:default:LlamaModel pid=17272) /usr/local/lib/python3.10/dist-packages/transformers/models/auto/auto_factory.py:472: FutureWarning: The `use_auth_token` argument is deprecated and will be removed in v5 of Transformers.
(ServeReplica:default:LlamaModel pid=17272)   warnings.warn(
Loading checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s]
Loading checkpoint shards:  50%|█████     | 1/2 [00:17<00:17, 17.90s/it]
(ServeController pid=17084) WARNING 2024-02-01 05:39:09,835 controller 17084 deployment_state.py:2171 - Deployment 'LlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading checkpoint shards: 100%|██████████| 2/2 [00:24<00:00, 12.36s/it]
(ServeReplica:default:LlamaModel pid=17272) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:362: UserWarning: `do_sample` is set to `False`. However, `temperature` is set to `0.9` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `temperature`. This was detected when initializing the generation config instance, which means the corresponding file may hold incorrect parameterization and should be fixed.
(ServeReplica:default:LlamaModel pid=17272)   warnings.warn(
(ServeReplica:default:LlamaModel pid=17272) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:367: UserWarning: `do_sample` is set to `False`. However, `top_p` is set to `0.6` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `top_p`. This was detected when initializing the generation config instance, which means the corresponding file may hold incorrect parameterization and should be fixed.
(ServeReplica:default:LlamaModel pid=17272)   warnings.warn(
(ServeReplica:default:LlamaModel pid=17272) ============================= HABANA PT BRIDGE CONFIGURATION =========================== 
(ServeReplica:default:LlamaModel pid=17272)  PT_HPU_LAZY_MODE = 1
(ServeReplica:default:LlamaModel pid=17272)  PT_RECIPE_CACHE_PATH = 
(ServeReplica:default:LlamaModel pid=17272)  PT_CACHE_FOLDER_DELETE = 0
(ServeReplica:default:LlamaModel pid=17272)  PT_HPU_RECIPE_CACHE_CONFIG = 
(ServeReplica:default:LlamaModel pid=17272)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(ServeReplica:default:LlamaModel pid=17272)  PT_HPU_LAZY_ACC_PAR_MODE = 1
(ServeReplica:default:LlamaModel pid=17272)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(ServeReplica:default:LlamaModel pid=17272) ---------------------------: System Configuration :---------------------------
(ServeReplica:default:LlamaModel pid=17272) Num CPU Cores : 156
(ServeReplica:default:LlamaModel pid=17272) CPU RAM       : 495094196 KB
(ServeReplica:default:LlamaModel pid=17272) ------------------------------------------------------------------------------
2024-02-01 05:39:25,873 SUCC scripts.py:483 -- Deployed Serve app successfully.
```

In another shell, use the following code to send requests to the deployment to perform generation tasks.

```{literalinclude} ../doc_code/intel_gaudi_inference_client.py
:language: python
:start-after: __main_code_start__
:end-before: __main_code_end__
```

Here is an example output:
```text
Once upon a time, in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.
In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a smile on her face, ready to face whatever adventures the day might bring.
One day, a wicked sorcerer cast a spell on the kingdom, causing all
in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.
In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a smile on her face, ready to face whatever adventures the day might bring.
One day, a wicked sorcerer cast a spell on the kingdom, causing all
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

The terminal should print logs as the deployment starts up:
```text
2024-02-01 06:08:51,170 INFO scripts.py:438 -- Running import path: 'deepspeed_demo:entrypoint'.
2024-02-01 06:08:54,143 INFO worker.py:1540 -- Connecting to existing Ray cluster at address: 10.111.128.177:6379...
2024-02-01 06:08:54,154 INFO worker.py:1715 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265 
(ServeController pid=44317) INFO 2024-02-01 06:08:54,348 controller 44317 deployment_state.py:1545 - Deploying new version of deployment DeepSpeedLlamaModel in application 'default'. Setting initial target number of replicas to 1.
(ServeController pid=44317) INFO 2024-02-01 06:08:54,457 controller 44317 deployment_state.py:1708 - Stopping 1 replicas of deployment 'DeepSpeedLlamaModel' in application 'default' with outdated versions.
(ServeController pid=44317) INFO 2024-02-01 06:08:57,326 controller 44317 deployment_state.py:2187 - Replica default#DeepSpeedLlamaModel#ToJmHV is stopped.
(ServeController pid=44317) INFO 2024-02-01 06:08:57,327 controller 44317 deployment_state.py:1829 - Adding 1 replica to deployment DeepSpeedLlamaModel in application 'default'.
(DeepSpeedInferenceWorker pid=48021) [WARNING|utils.py:198] 2024-02-01 06:09:12,355 >> optimum-habana v1.11.0.dev0 has been validated for SynapseAI v1.14.0 but the driver version is v1.15.0, this could lead to undefined behavior!
(DeepSpeedInferenceWorker pid=48016) /usr/local/lib/python3.10/dist-packages/habana_frameworks/torch/hpu/__init__.py:158: UserWarning: torch.hpu.setDeterministic is deprecated and will be removed in next release. Please use torch.use_deterministic_algorithms instead.
(DeepSpeedInferenceWorker pid=48016)   warnings.warn(
(DeepSpeedInferenceWorker pid=48019) [2024-02-01 06:09:14,005] [INFO] [real_accelerator.py:178:get_accelerator] Setting ds_accelerator to hpu (auto detect)
(DeepSpeedInferenceWorker pid=48019) [2024-02-01 06:09:16,908] [INFO] [logging.py:96:log_dist] [Rank -1] DeepSpeed info: version=0.12.4+hpu.synapse.v1.14.0, git-hash=fad45b2, git-branch=1.14.0
(DeepSpeedInferenceWorker pid=48019) [2024-02-01 06:09:16,910] [INFO] [logging.py:96:log_dist] [Rank -1] quantize_bits = 8 mlp_extra_grouping = False, quantize_groups = 1
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s]
(DeepSpeedInferenceWorker pid=48019) [2024-02-01 06:09:16,955] [WARNING] [comm.py:163:init_deepspeed_backend] HCCL backend in DeepSpeed not yet implemented
(DeepSpeedInferenceWorker pid=48019) [2024-02-01 06:09:16,955] [INFO] [comm.py:637:init_distributed] cdb=None
(DeepSpeedInferenceWorker pid=48018) [WARNING|utils.py:198] 2024-02-01 06:09:13,528 >> optimum-habana v1.11.0.dev0 has been validated for SynapseAI v1.14.0 but the driver version is v1.15.0, this could lead to undefined behavior! [repeated 7x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(ServeController pid=44317) WARNING 2024-02-01 06:09:27,403 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
(DeepSpeedInferenceWorker pid=48018) /usr/local/lib/python3.10/dist-packages/habana_frameworks/torch/hpu/__init__.py:158: UserWarning: torch.hpu.setDeterministic is deprecated and will be removed in next release. Please use torch.use_deterministic_algorithms instead. [repeated 7x across cluster]
(DeepSpeedInferenceWorker pid=48018)   warnings.warn( [repeated 7x across cluster]
Loading 15 checkpoint shards:   0%|          | 0/15 [00:00<?, ?it/s] [repeated 7x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:09:57,475 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:52<12:15, 52.53s/it]
(DeepSpeedInferenceWorker pid=48014) ============================= HABANA PT BRIDGE CONFIGURATION =========================== 
(DeepSpeedInferenceWorker pid=48014)  PT_HPU_LAZY_MODE = 1
(DeepSpeedInferenceWorker pid=48014)  PT_RECIPE_CACHE_PATH = 
(DeepSpeedInferenceWorker pid=48014)  PT_CACHE_FOLDER_DELETE = 0
(DeepSpeedInferenceWorker pid=48014)  PT_HPU_RECIPE_CACHE_CONFIG = 
(DeepSpeedInferenceWorker pid=48014)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(DeepSpeedInferenceWorker pid=48014)  PT_HPU_LAZY_ACC_PAR_MODE = 0
(DeepSpeedInferenceWorker pid=48014)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(DeepSpeedInferenceWorker pid=48014) ---------------------------: System Configuration :---------------------------
(DeepSpeedInferenceWorker pid=48014) Num CPU Cores : 156
(DeepSpeedInferenceWorker pid=48014) CPU RAM       : 495094196 KB
(DeepSpeedInferenceWorker pid=48014) ------------------------------------------------------------------------------
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:57<13:28, 57.75s/it] [repeated 2x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:10:27,504 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:   7%|▋         | 1/15 [00:58<13:42, 58.75s/it] [repeated 5x across cluster]
Loading 15 checkpoint shards:  13%|█▎        | 2/15 [01:15<07:21, 33.98s/it]
Loading 15 checkpoint shards:  13%|█▎        | 2/15 [01:16<07:31, 34.70s/it]
Loading 15 checkpoint shards:  20%|██        | 3/15 [01:35<05:34, 27.88s/it] [repeated 7x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:10:57,547 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  27%|██▋       | 4/15 [01:53<04:24, 24.03s/it] [repeated 8x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:11:27,625 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  27%|██▋       | 4/15 [01:54<04:21, 23.79s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  40%|████      | 6/15 [02:30<03:06, 20.76s/it] [repeated 9x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:11:57,657 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  40%|████      | 6/15 [02:29<03:05, 20.61s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  47%|████▋     | 7/15 [02:47<02:39, 19.88s/it]
Loading 15 checkpoint shards:  47%|████▋     | 7/15 [02:48<02:39, 19.90s/it]
Loading 15 checkpoint shards:  53%|█████▎    | 8/15 [03:06<02:17, 19.60s/it] [repeated 7x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:12:27,721 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  60%|██████    | 9/15 [03:26<01:56, 19.46s/it] [repeated 8x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:12:57,725 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  67%|██████▋   | 10/15 [03:27<01:09, 13.80s/it] [repeated 15x across cluster]
Loading 15 checkpoint shards:  73%|███████▎  | 11/15 [03:46<01:00, 15.14s/it]
Loading 15 checkpoint shards:  73%|███████▎  | 11/15 [03:45<01:00, 15.15s/it]
Loading 15 checkpoint shards:  80%|████████  | 12/15 [04:05<00:49, 16.47s/it] [repeated 7x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:13:27,770 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [04:24<00:34, 17.26s/it] [repeated 8x across cluster]
(ServeController pid=44317) WARNING 2024-02-01 06:13:57,873 controller 44317 deployment_state.py:2171 - Deployment 'DeepSpeedLlamaModel' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
Loading 15 checkpoint shards:  87%|████████▋ | 13/15 [04:25<00:34, 17.35s/it] [repeated 7x across cluster]
Loading 15 checkpoint shards:  93%|█████████▎| 14/15 [04:44<00:17, 17.55s/it]
Loading 15 checkpoint shards: 100%|██████████| 15/15 [05:02<00:00, 18.30s/it] [repeated 8x across cluster]
2024-02-01 06:14:24,054 SUCC scripts.py:483 -- Deployed Serve app successfully.
```

Use the same code snippet introduced in the single HPU example to send generation requests. Here's an example output:
```text
Once upon a time, there was a young woman named Sophia who lived in a small village nestled in the rolling hills of Tuscany. Sophia was a curious and adventurous soul, always eager to explore the world around her. One day, while wandering through the village, she stumbled upon a hidden path she had never seen before.
The path was overgrown with weeds and vines, and it looked as though it hadn't been traversed in years. But Sophia was intrigued, and she decided to follow it to see where it led. She pushed aside the branches and stepped onto the path
Once upon a time, there was a young woman named Sophia who lived in a small village nestled in the rolling hills of Tuscany. Sophia was a curious and adventurous soul, always eager to explore the world around her. One day, while wandering through the village, she stumbled upon a hidden path she had never seen before.
The path was overgrown with weeds and vines, and it looked as though it hadn't been traversed in years. But Sophia was intrigued, and she decided to follow it to see where it led. She pushed aside the branches and stepped onto the path
```

## Next Steps
See [llm-on-ray](https://github.com/intel/llm-on-ray) for more ways to customize and deploy LLMs at scale.
