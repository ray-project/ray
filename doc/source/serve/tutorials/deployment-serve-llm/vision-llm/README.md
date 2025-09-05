---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Deploying a vision LLM

A vision LLM can interpret images as well as text, enabling tasks like answering questions about charts, analyzing photos, or combining visuals with instructions. It extends LLMs beyond language to support multimodal reasoning and richer applications.  

This tutorial deploys a vision LLM using Ray Serve LLM.  

---

## Configure Ray Serve LLM

Make sure to set your Hugging Face token in the config file to access gated models.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.


```python
# serve_qwen_VL.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwen-VL",
        model_source="qwen/Qwen2.5-VL-7B-Instruct",
    ),
    accelerator_type="L40S",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it.
    # runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(max_model_len=8192),
)

app = build_openai_app({"llm_configs": [llm_config]})

```

**Note:** Before moving to a production setup, migrate to a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs: production guide](https://docs.ray.io/en/latest/serve/llm/serving-llms.html#production-deployment) for an example.

---

## Deploy locally

**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta’s Llama. Store it in `export HF_TOKEN=<YOUR-TOKEN-HERE>`

**Note:** Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_qwen_VL.py`.  

In a terminal, run:   


```bash
%%bash
serve run serve_qwen_VL:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Sending requests with images

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

Example cURL with image URL


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ \
        "model": "my-qwen-VL", \
        "messages": [ \
          { \
            "role": "user", \
            "content": [ \
              {"type": "text", "text": "What do you see in this image?"}, \
              {"type": "image_url", "image_url": { \
                "url": "http://images.cocodataset.org/val2017/000000039769.jpg" \
              }} \
            ] \
          } \
        ] \
      }'
```

Example Python with image URL:


```python
#client_url_image.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

response = client.chat.completions.create(
    model="my-qwen-VL",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What is in this image?"},
                {"type": "image_url", "image_url": {"url": "http://images.cocodataset.org/val2017/000000039769.jpg"}}
            ]
        }
    ],
    temperature=0.5,
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)
```

Example Python with local image:


```python
#client_local_image.py
from urllib.parse import urljoin
import base64
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

### From an image locally saved as `example.jpg`
# Load and encode image as base64
with open("example.jpg", "rb") as f:
    img_base64 = base64.b64encode(f.read()).decode()

response = client.chat.completions.create(
    model="my-qwen-VL",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What is in this image?"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"}}
            ]
        }
    ],
    temperature=0.5,
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)
```


---

### Shutdown 

Shutdown your LLM service:


```bash
%%bash
serve shutdown -y
```


---

## Deploy to production with Anyscale services

For production, it's recommended to use Anyscale services to deploy your Ray Serve app on a dedicated cluster without code changes. Anyscale provides scalability, fault tolerance, and load balancing, ensuring resilience against node failures, high traffic, and rolling updates. See [Deploying a small-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html#deploy-to-production-with-anyscale-services) for an example with a small-sized model like the *Qwen2.5-VL-7&nbsp;B-Instruct* used in this tutorial.

---

## Limiting images per prompt

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/stable/) as its backend engine. You can configure vLLM by passing parameters through the `engine_kwargs` section of your Serve LLM configuration. For a full list of supported options, see the [vLLM documentation](https://docs.vllm.ai/en/stable/configuration/engine_args.html#multimodalconfig).  

In particular, you can limit the number of images per request by setting `limit_mm_per_prompt` in your configuration.  
```yaml
applications:
- ...
  args:
    llm_configs:
        ...
        engine_kwargs:
          ...
          limit_mm_per_prompt: {"image": 3}
```

---

## Summary

In this tutorial, you deployed a vision LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM, deploy your service on your Ray cluster, and send requests with images.
