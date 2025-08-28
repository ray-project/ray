# Deploy a hybrid reasoning LLM

A hybrid reasoning model provides flexibility by allowing you to enable or disable reasoning as needed. You can use structured, step-by-step thinking for complex queries while skipping it for simpler ones, balancing accuracy with efficiency depending on the task.

This tutorial deploys a hybrid reasoning LLM using Ray Serve LLM.  

---

## Distinction with purely reasoning models

*Hybrid reasoning models* are reasoning-capable models that allow you to toggle the thinking process on and off. You can enable structured, step-by-step reasoning when needed but skip it for simpler queries to reduce latency. Purely reasoning models always apply their reasoning behavior, while hybrid models give you fine-grained control over when that reasoning is used.
<!-- vale Google.Acronyms = NO -->
| **Mode**         | **Core Behavior**                            | **Use Case Examples**                                               | **Limitation**                                    |
| ---------------- | -------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------- |
| **Thinking ON**  | Explicit multi-step thinking process | Math, coding, logic puzzles, multi-hop QA, CoT prompting | Slower response time, more tokens used.      |
| **Thinking OFF** | Direct answer generation                   | Casual queries, short instructions, single-step answers              | May struggle with complex reasoning or interpretability. |
<!-- vale Google.Acronyms = YES -->
**Note:** Reasoning often benefits from long context windows (32K up to +1M tokens), high token throughput, low-temperature decoding (greedy sampling), and strong instruction tuning or scratchpad-style reasoning.

To see an example of deploying a purely reasoning model like *QwQ-32&nbsp;B*, see [Deploying a Reasoning LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/reasoning-llm/notebook.html).

---

## Enable or disable thinking

Some hybrid reasoning models let you toggle their "thinking" mode on or off. This section explains when to use thinking mode versus skipping it, and shows how to control the setting in practice.

---
<!-- vale Vale.Terms = NO -->
### When to enable or disable thinking mode
<!-- vale Vale.Terms = YES -->
**Enable thinking mode for:**
- Complex, multi-step tasks that require reasoning, such as math, physics, or logic problems.
- Ambiguous queries or situations with incomplete information.
- Planning, workflow orchestration, or when the model needs to act as an "agent" coordinating other tools or models.
- Analyzing intricate data, images, or charts.
- In-depth code reviews or evaluating outputs from other AI systems (LLM as Judge approach).

**Disable thinking mode for:**
- Simple, well-defined, or routine tasks.
- Low latency and fast responses as the priority.
- Repetitive, straightforward steps within a larger automated workflow.

---

### How to enable or disable thinking mode

Toggle thinking mode varies by model and framework. Check the model's documentation to see how thinking is structured and controlled.

For example, to [control reasoning in Qwen-3](https://huggingface.co/Qwen/Qwen3-32B#switching-between-thinking-and-non-thinking-mode), you can:
* Add `"/think"` or `"/no_think"` in the prompt.
* Set `enable_thinking` in the request:
  `extra_body={"chat_template_kwargs": {"enable_thinking": ...}}`.

See [Send request with thinking enabled](#send-request-with-thinking-enabled) or [Send request with thinking disabled](#send-request-with-thinking-disabled) for practical examples.

---

## Parse reasoning outputs

In thinking mode, hybrid models often separate *reasoning* from the *final answer* using tags like `<think>...</think>`. Without a proper parser, this reasoning may end up in the `content` field instead of the dedicated `reasoning_content` field.  

To ensure the reasoning output is correctly parsed, configure a `reasoning_parser` in your Ray Serve LLM deployment. This tells vLLM how to isolate the model’s thought process from the rest of the output.
**Note:** For example, *Qwen-3* uses the `qwen3` parser. See the [vLLM docs](https://docs.vllm.ai/en/stable/features/reasoning_outputs.html#supported-models) or your model's documentation to find a supported parser, or [build your own](https://docs.vllm.ai/en/stable/features/reasoning_outputs.html#how-to-support-a-new-reasoning-model) if needed.

```yaml
applications:
- ...
  args:
    llm_configs:
      - model_loading_config:
          model_id: my-qwen-3-32b
          model_source: Qwen/Qwen3-32B
        ...
        engine_kwargs:
          ...
          reasoning_parser: qwen3 # <-- for Qwen-3 models
```

See [Configure Ray Serve LLM](#configure-ray-serve-llm) for a complete example.

**Example Response**  
When using a reasoning parser, the response is typically structured like this:

```python
ChatCompletionMessage(
    content="The temperature is...",
    ...,
    reasoning_content="Okay, the user is asking for the temperature today and tomorrow..."
)
```
And you can extract the content and reasoning like this
```python
response = client.chat.completions.create(
  ...
)

print(f"Content: {response.choices[0].message.content}")
print(f"Reasoning: {response.choices[0].message.reasoning_content}")
```

---

## Configure Ray Serve LLM

Set your Hugging Face token in the config file to access gated models.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

Set `tensor_parallel_size` to distribute the model's weights among 8 GPUs in the node.  


```python
#serve_qwen_3_32b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwen-3-32b",
        model_source="Qwen/Qwen3-32B",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it
    #runtime_env=dict(
    #    env_vars={
    #        "HF_TOKEN": os.environ.get("HF_TOKEN")
    #    }
    #),
    engine_kwargs=dict(
        tensor_parallel_size=8,
        max_model_len=32768,
        reasoning_parser='qwen3'
    )
)
app = build_openai_app({"llm_configs": [llm_config]})
```

**Note:** Before moving to a production setup, it's recommended to switch to a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html). This makes your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines for example. See [Serving LLMs: Production Guide](https://docs.ray.io/en/latest/serve/llm/serving-llms.html#production-deployment) for an example.

---

## Deploy locally

**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta’s Llama. Store it in `export HF_TOKEN=<YOUR-TOKEN-HERE>`.

**Note:** Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_qwen_3_32b.py`.  

In a terminal, run:  


```bash
%%bash
serve run serve_qwen_3_32b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`

Use the `model_id` defined in your config (here, `my-qwen-3-32b`) to query your model. Here are some examples on how to send request to a Qwen-3 deployment with thinking enabled or disabled. 

---

### Send request with thinking disabled

You can disable thinking in Qwen-3 by either adding a `/no_think` tag in the prompt or by forwarding `enable_thinking: False` to the vLLM inference engine.  

Example Curl with `/no_think`


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer FAKE_KEY" \
  -d '{ \
        "model": "my-qwen-3-32b", \
        "messages": [{"role": "user", "content": "What is greater between 7.8 and 7.11 ? /no_think"}] \
      }'
```

Example Python with `enable_thinking: False`


```python
#client_thinking_disabled.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwen-3-32b",
    messages=[
        {"role": "user", "content": "What's the capital of France ?"}
    ],
    extra_body={"chat_template_kwargs": {"enable_thinking": False}}
)

print(f"Reasoning: \n{response.choices[0].message.reasoning_content}\n\n")
print(f"Answer: \n {response.choices[0].message.content}")
```

Notice the `reasoning_content` is empty here. 
**Note:** Depending on your model's documentation, empty could mean `None`, an empty string or even empty tags `"<think></think>"`

---

### Send request with thinking enabled
 
You can enable thinking in Qwen-3 by either adding a `/think` tag in the prompt or by forwarding `enable_thinking: True` to the vLLM inference engine.  

Example Curl with `/think`


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer FAKE_KEY" \
  -d '{ \
        "model": "my-qwen-3-32b", \
        "messages": [{"role": "user", "content": "What is greater between 7.8 and 7.11 ? /think"}] \
      }'
```

 Example Python with `enable_thinking: True`


```python
#client_thinking_enabled.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwen-3-32b",
    messages=[
        {"role": "user", "content": "What's the capital of France ?"}
    ],
    extra_body={"chat_template_kwargs": {"enable_thinking": True}}
)

print(f"Reasoning: \n{response.choices[0].message.reasoning_content}\n\n")
print(f"Answer: \n {response.choices[0].message.content}")
```

If you configure a valid reasoning parser, the reasoning output should appear in the `reasoning_content` field of the response message. Otherwise, it may be included in the main `content` field, typically wrapped in `<think>...</think>` tags. See [Parse reasoning outputs](#parse-reasoning-outputs) for more information.

---

### Shutdown 

Shutdown your LLM service:


```bash
%%bash
serve shutdown -y
```


---

## Deploy to production with Anyscale Services

For production, it's recommended to use Anyscale Services to deploy your Ray Serve app on a dedicated cluster without code changes. Anyscale provides scalability, fault tolerance, and load balancing, ensuring resilience against node failures, high traffic, and rolling updates. See [Deploying a medium-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/medium-size-llm/README.html#production-deployment-with-anyscale-service) for an example with a medium-size model like the *Qwen-32b* used here.

---

## Stream reasoning content

In thinking mode, hybrid reasoning models may take longer to begin generating the main content. You can stream their intermediate reasoning output in the same way as the main content.  


```python
#client_streaming.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwen-3-32b",
    messages=[
        {"role": "user", "content": "What's the capital of France ?"}
    ],
    extra_body={"chat_template_kwargs": {"enable_thinking": True}}
)

print(f"Reasoning: \n{response.choices[0].message.reasoning_content}\n\n")
print(f"Answer: \n {response.choices[0].message.content}")
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwen-3-32b",
    messages=[
        {"role": "user", "content": "I need to plan a trip to Paris from Seattle. Can you help me research flight costs, create an itinerary for 3 days, and suggest restaurants based on my dietary restrictions (vegetarian)?"}
    ],
    extra_body={"chat_template_kwargs": {"enable_thinking": True}},
    stream=True
)

# Stream 
for chunk in response:
    # Stream reasoning content
    if hasattr(chunk.choices[0].delta, "reasoning_content"):
        data_reasoning = chunk.choices[0].delta.reasoning_content
        if data_reasoning:
            print(data_reasoning, end="", flush=True)
    # Later, stream the final answer
    if hasattr(chunk.choices[0].delta, "content"):
        data_content = chunk.choices[0].delta.content
        if data_content:
            print(data_content, end="", flush=True)
```


---

## Summary

In this tutorial, you deployed a hybrid reasoning LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM with the right reasoning parser, deploy your service on your Ray cluster, and how to send requests, and how to parse reasoning outputs in the response.
