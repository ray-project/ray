# Serving an inference model on Gaudi

## Environment Setup
To run the examples introduced here, using a prebuilt container is recommended. First, ensure that the drivers and container runtime
are installed, if not, refer to this [guide](https://docs.habana.ai/en/latest/Installation_Guide/Bare_Metal_Fresh_OS.html?highlight=installer#run-using-containers). You can run `hl-smi` to verify your installation.

Next, you can start the container like this:
```bash
docker pull vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
docker run -it --runtime=habana -e HABANA_VISIBLE_DEVICES=all -e OMPI_MCA_btl_vader_single_copy_mechanism=none --cap-add=sys_nice --net=host --ipc=host vault.habana.ai/gaudi-docker/1.14.0/ubuntu22.04/habanalabs/pytorch-installer-2.1.1:latest
```

Notice that you may want to mount the directory containing the examples into the container, as well as the models.

Inside the container, run
```bash
# ray 2.9.1 has some problem with latest fastapi
pip install ray[tune,serve] fastapi==0.104
pip install git+https://github.com/huggingface/optimum-habana.git
# 1.14.0 should be replaced with the driver version of the container
pip install git+https://github.com/HabanaAI/DeepSpeed.git@1.14.0
# only needed by the deepspeed example
export RAY_EXPERIMENTAL_NOSET_HABANA_VISIBLE_MODULES=1
```

Now, start a ray cluster via `ray start --head` and you are ready to run the examples.

## Single Card Example

This example shows how to deploy Llama2-7b model on Gaudi for inference. 

First, define a deployment which serves Llama2-7b model using HPU.

```{literalinclude} ../doc_code/hpu_inference_serve.py
:language: python
:start-after: __model_def_start__
:end-before: __model_def_end__
```

Then, start the deployment. Now, you can send requests to perform generation taks.

```{literalinclude} ../doc_code/hpu_inference_serve.py
:language: python
:start-after: __main_code_start__
:end-before: __main_code_end__
```

You can find the code [here](/doc/source/serve/doc_code/hpu_inference_serve.py). Run it like this:
```bash
python hpu_inference_serve.py
# If path of the model is not default huggingface cache dir
# Add the path as an argument
python hpu_inference_serve.py /path/to/model/bin
```

If it succeeds, you should see 

```text
(ServeReplica:default:LlamaModel pid=39861)   warnings.warn(
Loading checkpoint shards:   0%|          | 0/2 [00:00<?, ?it/s]
Loading checkpoint shards:  50%|█████     | 1/2 [00:05<00:05,  5.57s/it]
Loading checkpoint shards: 100%|██████████| 2/2 [00:07<00:00,  3.67s/it]
(ServeReplica:default:LlamaModel pid=39861) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:362: UserWarning: `do_sample` is set to `False`. However, `temperature` is set to `0.9` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `temperature`. This was detected when initializing the generation config instance, which means the corresponding file may hold incorrect parameterization and should be fixed.
(ServeReplica:default:LlamaModel pid=39861)   warnings.warn(
(ServeReplica:default:LlamaModel pid=39861) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:367: UserWarning: `do_sample` is set to `False`. However, `top_p` is set to `0.6` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `top_p`. This was detected when initializing the generation config instance, which means the corresponding file may hold incorrect parameterization and should be fixed.
(ServeReplica:default:LlamaModel pid=39861)   warnings.warn(
(ServeReplica:default:LlamaModel pid=39861) ============================= HABANA PT BRIDGE CONFIGURATION =========================== 
(ServeReplica:default:LlamaModel pid=39861)  PT_HPU_LAZY_MODE = 1
(ServeReplica:default:LlamaModel pid=39861)  PT_RECIPE_CACHE_PATH = 
(ServeReplica:default:LlamaModel pid=39861)  PT_CACHE_FOLDER_DELETE = 0
(ServeReplica:default:LlamaModel pid=39861)  PT_HPU_RECIPE_CACHE_CONFIG = 
(ServeReplica:default:LlamaModel pid=39861)  PT_HPU_MAX_COMPOUND_OP_SIZE = 9223372036854775807
(ServeReplica:default:LlamaModel pid=39861)  PT_HPU_LAZY_ACC_PAR_MODE = 1
(ServeReplica:default:LlamaModel pid=39861)  PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES = 0
(ServeReplica:default:LlamaModel pid=39861) ---------------------------: System Configuration :---------------------------
(ServeReplica:default:LlamaModel pid=39861) Num CPU Cores : 156
(ServeReplica:default:LlamaModel pid=39861) CPU RAM       : 495094188 KB
(ServeReplica:default:LlamaModel pid=39861) ------------------------------------------------------------------------------
Model is deployed successfully at http://127.0.0.1:8000/
Press Enter to start inference
(ServeReplica:default:LlamaModel pid=39861) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:362: UserWarning: `do_sample` is set to `False`. However, `temperature` is set to `0.9` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `temperature`.
(ServeReplica:default:LlamaModel pid=39861)   warnings.warn(
(ServeReplica:default:LlamaModel pid=39861) /usr/local/lib/python3.10/dist-packages/transformers/generation/configuration_utils.py:367: UserWarning: `do_sample` is set to `False`. However, `top_p` is set to `0.6` -- this flag is only used in sample-based generation modes. You should set `do_sample=True` or unset `top_p`.
(ServeReplica:default:LlamaModel pid=39861)   warnings.warn(
Once upon a time, in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.
In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a smile on her face, ready to face whatever adventures the day might bring.
One day, a wicked sorcerer cast a spell on the kingdom, causing all
Press Enter to start streaming inference(ServeReplica:default:LlamaModel pid=39861) INFO 2024-01-29 07:41:47,072 default_LlamaModel xCtlqq 79bc3fcc-8734-4295-aa23-98af8e1dbdeb / replica.py:772 - __CALL__ OK 4070.8ms

in a far-off land, there was a magical kingdom called "Happily Ever Laughter." It was a place where laughter was the key to unlocking all the joys of life, and where everyone lived in perfect harmony.
In this kingdom, there was a beautiful princess named Lily. She was kind, gentle, and had a heart full of laughter. Every day, she would wake up with a smile on her face, ready to face whatever adventures the day might bring.
One day, a wicked sorcerer cast a spell on the kingdom, causing all
(ServeReplica:default:LlamaModel pid=39861) INFO 2024-01-29 07:41:54,186 default_LlamaModel xCtlqq 7004ccff-3337-495e-b6e0-906fbda11e51 / replica.py:772 - __CALL__ OK 2650.0ms
```

## Multiple Card Example

This example shows how to deploy a Llama2-70b model using 8 HPU orchestrated by DeepSpeed. 

If you haven't cached Llama2-70b model, please do so and mount the path into the container. Notice that it will need about 257G space. You can cache the model by running the following

```python
snapshot_download(
    "meta-llama/Llama-2-70b-chat-hf",
    # replace the path if you need
    cache_dir=os.getenv("TRANSFORMERS_CACHE", None),
    token=""
)
```

In this example, the deployment actor will send prompts to the deepspeed workers, which is defined as ray actors:

```{literalinclude} ../doc_code/hpu_inference_serve_deepspeed.py
:language: python
:start-after: __worker_def_start__
:end-before: __worker_def_end__
```

For more details, please see [here](/doc/source/serve/doc_code/hpu_inference_serve_deepspeed.py).

Similarly, you can run this example like `python hpu_inference_serve_deepspeed.py 8`. Again, append the path to the model binaries if needed. Press enter to start inference after you see `Model is deployed successfully at http://127.0.0.1:8000/`, and press enter again to start streaming inference.
