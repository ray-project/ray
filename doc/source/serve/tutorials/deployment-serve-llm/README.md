---
orphan: true
---

# Quickstarts for LLM serving

These guides provide a fast path to serving LLMs using Ray Serve on Anyscale, with focused tutorials for different deployment scales, from single-GPU setups to multi-node clusters.

Each tutorial includes development and production setups, tips for configuring your cluster, and guidance on monitoring and scaling with Ray Serve.

## Tutorial categories

**[Small-sized LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/small-size-llm/README.html)**  
Deploy small-sized models on a single GPU, such as Llama 3 8&nbsp;B, Mistral 7&nbsp;B, or Phi-2.  

---

**[Medium-sized LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/medium-size-llm/README.html)**  
Deploy medium-sized models using tensor parallelism across 4—8 GPUs on a single node, such as Llama 3 70&nbsp;B, Qwen 14&nbsp;B, Mixtral 8x7&nbsp;B.  

---

**[Large-sized LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/large-size-llm/README.html)**  
Deploy massive models using pipeline parallelism across a multi-node cluster, such as Deepseek-R1 or Llama-Nemotron-253&nbsp;B.  

---

**[Vision LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/vision-llm/README.html)**  
Deploy models with image and text input such as Qwen 2.5-VL-7&nbsp;B-Instruct, MiniGPT-4, or Pixtral-12&nbsp;B.  

---

**[Reasoning LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/reasoning-llm/README.html)**  
Deploy models with reasoning capabilities designed for long-context tasks, coding, or tool use, such as QwQ-32&nbsp;B.  

---

**[Hybrid thinking LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/hybrid-reasoning-llm/README.html)**  
Deploy models that can switch between reasoning and non-reasoning modes for flexible usage, such as Qwen-3.

```{toctree}
:hidden:

small-size-llm/notebook
medium-size-llm/notebook
large-size-llm/notebook
vision-llm/notebook
reasoning-llm/notebook
hybrid-reasoning-llm/notebook
```
