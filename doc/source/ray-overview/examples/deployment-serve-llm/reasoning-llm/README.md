# Deploy a reasoning LLM

A reasoning LLM handles tasks that require deeper analysis or step-by-step thought. It generates intermediate reasoning before arriving at a final answer, making it better suited for situations where careful logic or structured problem-solving is more important than speed or efficiency.

This tutorial deploys a reasoning LLM using Ray Serve LLM.  

---

## Compare reasoning and non-reasoning models

Reasoning models simulate step-by-step, structured thought processes to solve complex tasks like math, multi-hop QA, or code generation. In contrast, non-reasoning models provide fast, direct responses and focus on fluency or instruction following without explicit intermediate reasoning. The key distinction lies in whether the model attempts to "think through" the problem before answering.

| **Model Type**          | **Core Behavior**                    | **Use Case Examples**                                    | **Limitation**                                        |
| ----------------------- | ------------------------------------ | -------------------------------------------------------- | ----------------------------------------------------- |
| **Reasoning Model**     | Explicit multi-step thinking process | Math, coding, logic puzzles, multi-hop QA, CoT prompting | Slower response time, more tokens used.                |
| **Non-Reasoning Model** | Direct answer generation             | Casual queries, short instructions, single-step answers  | May struggle with complex reasoning or interpretability. |

Many reasoning-capable models structure their outputs with special markers such as `<think>` tags, or expose reasoning traces inside dedicated fields like `reasoning_content` in the OpenAI API response. Always check the model's documentation to see how thinking is structured and controlled.

**Note:** Reasoning LLMs often benefit from long context windows (32K up to +1M tokens), high token throughput, low-temperature decoding (greedy sampling), and strong instruction tuning or scratchpad-style reasoning.

---

### Choose when to use reasoning models

Whether you should use a reasoning model depends on how much information your prompt already provides.

If your input is clear and complete, a standard model is usually faster and more efficient. If your input is ambiguous or complex, a reasoning model works better because it can work through the problem step by step and fill in gaps through intermediate reasoning.

---

## Parse reasoning outputs

Reasoning models often separate *reasoning* from the *final answer* using tags like `<think>...</think>`. Without a proper parser, this reasoning may end up in the `content` field instead of the dedicated `reasoning_content` field.

To extract reasoning correctly, configure a `reasoning_parser` in your Ray Serve deployment. This tells vLLM how to isolate the model’s thought process from the rest of the output.
**Note:** For example, *QwQ* uses the `deepseek-r1` parser. Other models may require different parsers. See the [vLLM docs](https://docs.vllm.ai/en/stable/features/reasoning_outputs.html#supported-models) or your model's documentation to find a supported parser, or [build your own](https://docs.vllm.ai/en/stable/features/reasoning_outputs.html#how-to-support-a-new-reasoning-model) if needed.

```yaml
applications:
- name: reasoning-llm-app
  ...
  args:
    llm_configs:
      - model_loading_config:
          model_id: my-qwq-32B
          model_source: Qwen/QwQ-32B
        ...
        engine_kwargs:
          ...
          reasoning_parser: deepseek_r1 # <-- for QwQ models
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

Set `tensor_parallel_size=8` to distribute the model's weights among 8 GPUs in the node. 


```python
#serve_qwq_32b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-qwq-32B",
        model_source="Qwen/QwQ-32B",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    ### Uncomment if your model is gated and need your Huggingface Token to access it
    #runtime_env=dict(
    #    env_vars={
    #        "HF_TOKEN": os.environ.get("HF_TOKEN")
    #    }
    #),
    engine_kwargs=dict(
        tensor_parallel_size=8,
        max_model_len=32768,
        reasoning_parser='deepseek_r1'
    )
)

app = build_openai_app({"llm_configs": [llm_config]})
```

**Note:** Before moving to a production setup, migrate to using a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs: Production Guide](https://docs.ray.io/en/latest/serve/llm/serving-llms.html#production-deployment) for an example.

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

### Launch the service

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_qwq_32b.py`.  

In a terminal, run:  


```bash
%%bash
serve run serve_qwq_32b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Send requests

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

Example Curl


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ \
        "model": "my-qwq-32B", \
        "messages": [{"role": "user", "content": "Pick three random words with 3 syllables each and count the number of R'\''s in each of them"}] \
      }'
```

Example Python


```python
#client.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

response = client.chat.completions.create(
    model="my-qwq-32B",
    messages=[
        {"role": "user", "content": "What is the sum of all even numbers between 1 and 100?"}
    ]
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

For production, use Anyscale Services to deploy your Ray Serve app on a dedicated cluster without code changes. Anyscale provides scalability, fault tolerance, and load balancing, ensuring resilience against node failures, high traffic, and rolling updates. See [Deploying a medium-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/medium-size-llm/README.html#deploy-to-production-with-anyscale-services) for an example with a medium-size model like the *QwQ-32&nbsp;B* used here.

---

## Stream reasoning content

Reasoning models may take longer to begin generating the main content. You can stream their intermediate reasoning output in the same way as the main content.  


```python
#client_streaming.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = <YOUR-TOKEN-HERE>
base_url = <YOUR-ENDPOINT-HERE>

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwq-32B",
    messages=[
        {"role": "user", "content": "I need to plan a trip to Paris from Seattle. Can you help me research flight costs, create an itinerary for 3 days, and suggest restaurants based on my dietary restrictions (vegetarian)?"}
    ],
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

In this tutorial, you deployed a reasoning LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM with the right reasoning parser, deploy your service on your Ray cluster, and how to send requests, and how to parse reasoning outputs in the response.
