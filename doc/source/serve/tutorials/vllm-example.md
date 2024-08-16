---
orphan: true
---

(serve-vllm-tutorial)=

# Serve a Large Language Model with vLLM
This example runs a large language model with Ray Serve using [vLLM](https://docs.vllm.ai/en/latest/), a popular open-source library for serving LLMs. It uses the [OpenAI Chat Completions API](https://platform.openai.com/docs/guides/text-generation/chat-completions-api), which easily integrates with other LLM tools. The example also sets up multi-GPU serving with Ray Serve using placement groups. For more advanced features like multi-lora support with serve multiplexing, JSON mode function calling and further performance improvements, try LLM deployment solutions on [Anyscale](https://www.anyscale.com/). 

To run this example, install the following:

```bash
pip install "ray[serve]" requests vllm
```

This example uses the [NousResearch/Meta-Llama-3-8B-Instruct](https://huggingface.co/NousResearch/Meta-Llama-3-8B-Instruct) model. Save the following code to a file named `llm.py`.

The Serve code is as follows:
```{literalinclude} ../doc_code/vllm_openai_example.py
:caption: llm.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Use `serve run llm:build_app model="NousResearch/Meta-Llama-3-8B-Instruct" tensor-parallel-size=2` to start the Serve app.

:::{note}
This example uses Tensor Parallel size of 2, which means Ray Serve deploys the model to Ray Actors across 2 GPUs using placement groups.
:::


Use the following code to send requests:
```{literalinclude} ../doc_code/vllm_openai_example.py
:caption: query.py
:language: python
:start-after: __query_example_begin__
:end-before: __query_example_end__
```
