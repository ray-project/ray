# Serve a model on Intel Habana Gaudi

[Habana Gaudi AI Processors (HPUs)](https://habana.ai) are AI hardware accelerators designed by Habana Labs. For more information, see [Gaudi Architecture](https://docs.habana.ai/en/latest/Gaudi_Overview/index.html) and [Gaudi Developer Docs](https://developer.habana.ai/).

This tutorial shows how to deploy [Llama2-7b](https://huggingface.co/meta-llama/Llama-2-7b-chat-hf) using a single HPU:

* How to load a model in an HPU

* How to perform generation on an HPU

* How to enable HPU Graph optimizations

This tutorial helps you serve a large language model on HPUs.

## Environment setup

We recommend using a prebuilt container to run these examples. To run a container, you need Docker. See [Install Docker Engine](https://docs.docker.com/engine/install/) for installation instructions.

Next, follow [Run Using Containers](https://docs.habana.ai/en/latest/Installation_Guide/Bare_Metal_Fresh_OS.html?highlight=installer#run-using-containers) to install the Habana drivers and container runtime. To verify your installation, start a shell and run `hl-smi`. It should print status information about the HPUs on the machine:

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

Next, start the Habana container:
```bash
docker pull vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
```

To follow the examples in this tutorial, mount the directory containing the examples and models into the container. Inside the container, run:
```bash
pip install ray[tune,serve]
pip install git+https://github.com/huggingface/optimum-habana.git
```

Start Ray in the container with `ray start --head`. You are now ready to run the examples.

## Running a model on a single HPU

This example shows how to deploy a Llama2-7b model on an HPU for inference. 

First, define a deployment that serves a Llama2-7b model using an HPU. Note that we enable [HPU graph optimizations](https://docs.habana.ai/en/latest/Gaudi_Overview/SynapseAI_Software_Suite.html?highlight=graph#graph-compiler-and-runtime) for better performance.

```{literalinclude} ../doc_code/hpu_inference_serve.py
:language: python
:start-after: __model_def_start__
:end-before: __model_def_end__
```

Copy the code above and save it as `hpu_inference_serve.py`. Start the deployment like this:

```bash
serve run hpu_inference_serve:entrypoint
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

```{literalinclude} ../doc_code/hpu_inference_client.py
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
