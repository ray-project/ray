---
orphan: true
---

# Serve an Inference with Stable Diffusion Model on AWS NeuronCores Using FastAPI
This example uses a precompiled Stable Diffusion XL model and deploys on an AWS Inferentia2 (Inf2)
instance using Ray Serve and FastAPI.


:::{note}
  Before starting this example: 
  * Set up [PyTorch Neuron](https://awsdocs-neuron.readthedocs-hosted.com/en/latest/general/setup/torch-neuronx.html#setup-torch-neuronx)
  * Install AWS NeuronCore drivers and tools, and torch-neuronx based on the instance-type

:::

```bash
pip install "optimum-neuron==0.0.13" "diffusers==0.21.4"
pip install "ray[serve]" requests transformers
```

This example uses the [Stable Diffusion-XL](https://huggingface.co/aws-neuron/stable-diffusion-xl-base-1-0-1024x1024) model and [FastAPI](https://fastapi.tiangolo.com/).
This model is compiled with AWS Neuron and is ready to run inference. However, you can choose a different Stable Diffusion model and compile it to be compatible for running inference on AWS Inferentia2
instances.

The model in this example is ready for deployment. Save the following code to a file named aws_neuron_core_inference_serve_stable_diffusion.py.

Use `serve run aws_neuron_core_inference_serve_stable_diffusion:entrypoint` to start the Serve application.
```{literalinclude} ../doc_code/aws_neuron_core_inference_serve_stable_diffusion.py
:language: python
:start-after: __neuron_serve_code_start__
:end-before: __neuron_serve_code_end__
```


You should see the following log messages when a deployment using RayServe is successful:
```text
2024-02-07 17:53:28,299	INFO worker.py:1715 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265 
(ProxyActor pid=25282) INFO 2024-02-07 17:53:31,751 proxy 172.31.10.188 proxy.py:1128 - Proxy actor fd464602af1e456162edf6f901000000 starting on node 5a8e0c24b22976f1f7672cc54f13ace25af3664a51429d8e332c0679.
(ProxyActor pid=25282) INFO 2024-02-07 17:53:31,755 proxy 172.31.10.188 proxy.py:1333 - Starting HTTP server on node: 5a8e0c24b22976f1f7672cc54f13ace25af3664a51429d8e332c0679 listening on port 8000
(ProxyActor pid=25282) INFO:     Started server process [25282]
(ServeController pid=25233) INFO 2024-02-07 17:53:31,921 controller 25233 deployment_state.py:1545 - Deploying new version of deployment StableDiffusionV2 in application 'default'. Setting initial target number of replicas to 1.
(ServeController pid=25233) INFO 2024-02-07 17:53:31,922 controller 25233 deployment_state.py:1545 - Deploying new version of deployment APIIngress in application 'default'. Setting initial target number of replicas to 1.
(ServeController pid=25233) INFO 2024-02-07 17:53:32,024 controller 25233 deployment_state.py:1829 - Adding 1 replica to deployment StableDiffusionV2 in application 'default'.
(ServeController pid=25233) INFO 2024-02-07 17:53:32,029 controller 25233 deployment_state.py:1829 - Adding 1 replica to deployment APIIngress in application 'default'.
Fetching 20 files: 100%|██████████| 20/20 [00:00<00:00, 195538.65it/s]
(ServeController pid=25233) WARNING 2024-02-07 17:54:02,114 controller 25233 deployment_state.py:2171 - Deployment 'StableDiffusionV2' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
(ServeController pid=25233) WARNING 2024-02-07 17:54:32,170 controller 25233 deployment_state.py:2171 - Deployment 'StableDiffusionV2' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
(ServeController pid=25233) WARNING 2024-02-07 17:55:02,344 controller 25233 deployment_state.py:2171 - Deployment 'StableDiffusionV2' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
(ServeController pid=25233) WARNING 2024-02-07 17:55:32,418 controller 25233 deployment_state.py:2171 - Deployment 'StableDiffusionV2' in application 'default' has 1 replicas that have taken more than 30s to initialize. This may be caused by a slow __init__ or reconfigure method.
2024-02-07 17:55:46,263	SUCC scripts.py:483 -- Deployed Serve app successfully.
```

Use the following code to send requests:
```python
import requests

prompt = "a zebra is dancing in the grass, river, sunlit"
input = "%20".join(prompt.split(" "))
resp = requests.get(f"http://127.0.0.1:8000/imagine?prompt={input}")
print("Write the response to `output.png`.")
with open("output.png", "wb") as f:
    f.write(resp.content)
```

You should see the following log messages when a request is sent to the endpoint:
```text
(ServeReplica:default:StableDiffusionV2 pid=25320) Prompt:  a zebra is dancing in the grass, river, sunlit
  0%|          | 0/50 [00:00<?, ?it/s]2 pid=25320) 
  2%|▏         | 1/50 [00:00<00:14,  3.43it/s]320) 
  4%|▍         | 2/50 [00:00<00:13,  3.62it/s]320) 
  6%|▌         | 3/50 [00:00<00:12,  3.73it/s]320) 
  8%|▊         | 4/50 [00:01<00:12,  3.78it/s]320) 
 10%|█         | 5/50 [00:01<00:11,  3.81it/s]320) 
 12%|█▏        | 6/50 [00:01<00:11,  3.82it/s]320) 
 14%|█▍        | 7/50 [00:01<00:11,  3.83it/s]320) 
 16%|█▌        | 8/50 [00:02<00:10,  3.84it/s]320) 
 18%|█▊        | 9/50 [00:02<00:10,  3.85it/s]320) 
 20%|██        | 10/50 [00:02<00:10,  3.85it/s]20) 
 22%|██▏       | 11/50 [00:02<00:10,  3.85it/s]20) 
 24%|██▍       | 12/50 [00:03<00:09,  3.86it/s]20) 
 26%|██▌       | 13/50 [00:03<00:09,  3.86it/s]20) 
 28%|██▊       | 14/50 [00:03<00:09,  3.85it/s]20) 
 30%|███       | 15/50 [00:03<00:09,  3.85it/s]20) 
 32%|███▏      | 16/50 [00:04<00:08,  3.85it/s]20) 
 34%|███▍      | 17/50 [00:04<00:08,  3.85it/s]20) 
 36%|███▌      | 18/50 [00:04<00:08,  3.85it/s]20) 
 38%|███▊      | 19/50 [00:04<00:08,  3.86it/s]20) 
 40%|████      | 20/50 [00:05<00:07,  3.85it/s]20) 
 42%|████▏     | 21/50 [00:05<00:07,  3.85it/s]20) 
 44%|████▍     | 22/50 [00:05<00:07,  3.85it/s]20) 
 46%|████▌     | 23/50 [00:06<00:07,  3.81it/s]20) 
 48%|████▊     | 24/50 [00:06<00:06,  3.81it/s]20) 
 50%|█████     | 25/50 [00:06<00:06,  3.82it/s]20) 
 52%|█████▏    | 26/50 [00:06<00:06,  3.83it/s]20) 
 54%|█████▍    | 27/50 [00:07<00:05,  3.84it/s]20) 
 56%|█████▌    | 28/50 [00:07<00:05,  3.84it/s]20) 
 58%|█████▊    | 29/50 [00:07<00:05,  3.84it/s]20) 
 60%|██████    | 30/50 [00:07<00:05,  3.84it/s]20) 
 62%|██████▏   | 31/50 [00:08<00:04,  3.84it/s]20) 
 64%|██████▍   | 32/50 [00:08<00:04,  3.84it/s]20) 
 66%|██████▌   | 33/50 [00:08<00:04,  3.85it/s]20) 
 68%|██████▊   | 34/50 [00:08<00:04,  3.85it/s]20) 
 70%|███████   | 35/50 [00:09<00:03,  3.84it/s]20) 
 72%|███████▏  | 36/50 [00:09<00:03,  3.84it/s]20) 
 74%|███████▍  | 37/50 [00:09<00:03,  3.84it/s]20) 
 76%|███████▌  | 38/50 [00:09<00:03,  3.84it/s]20) 
 78%|███████▊  | 39/50 [00:10<00:02,  3.84it/s]20) 
 80%|████████  | 40/50 [00:10<00:02,  3.84it/s]20) 
 82%|████████▏ | 41/50 [00:10<00:02,  3.84it/s]20) 
 84%|████████▍ | 42/50 [00:10<00:02,  3.84it/s]20) 
 86%|████████▌ | 43/50 [00:11<00:01,  3.84it/s]20) 
 88%|████████▊ | 44/50 [00:11<00:01,  3.84it/s]20) 
 90%|█████████ | 45/50 [00:11<00:01,  3.84it/s]20) 
 92%|█████████▏| 46/50 [00:11<00:01,  3.85it/s]20) 
 94%|█████████▍| 47/50 [00:12<00:00,  3.85it/s]20) 
 96%|█████████▌| 48/50 [00:12<00:00,  3.84it/s]20) 
 98%|█████████▊| 49/50 [00:12<00:00,  3.84it/s]20) 
100%|██████████| 50/50 [00:13<00:00,  3.83it/s]20) 
(ServeReplica:default:StableDiffusionV2 pid=25320) INFO 2024-02-07 17:58:36,604 default_StableDiffusionV2 OXPzZm 33133be7-246f-4492-9ab6-6a4c2666b306 /imagine replica.py:772 - GENERATE OK 14167.2ms
```


The app saves the `output.png` file locally. The following is an example of an output image.
![image](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/stable_diffusion_inferentia2_output.png)
