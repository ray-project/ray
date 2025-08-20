# Quick-starts with LLM Serving on Anyscale

These guides provide a fast path to serving LLMs using Ray Serve on Anyscale, with focused tutorials for different deployment scales, from single-GPU setups to multi-node clusters.

Each tutorial includes development and production setups, tips for configuring your cluster, and guidance on monitoring and scaling with Ray Serve.

## Tutorial Categories

**[Small size LLM deployment](https://github.com/ray-project/ray/tree/master/doc/source/ray-overview/examples/deployment-serve-llm/notebooks/small-size-llm.ipynb)**  
Deploy small size models using tensor parallelism across 4—8 GPUs on a single node, such as Llama 3 8B, Mistral 7B, or Phi-2.  

---

**[Medium size LLM deployment](https://github.com/ray-project/ray/tree/master/doc/source/ray-overview/examples/deployment-serve-llm/notebooks/medium-size-llm.ipynb)**  
Deploy medium size models using tensor parallelism across 4—8 GPUs on a single node, such as Llama 3 70B, Qwen 14B, Mixtral 8x7B.  

---

**[Large size LLM deployment](#)**  
Deploy massive models using pipeline parallelism across a multi-node cluster, such as Deepseek-R1 or Llama-Nemotron-253B.  

---

**[Vision LLM deployment](#)**  
Deploy models with image + text input such as Qwen 2.5-VL-7B-Instruct, MiniGPT-4, or Pixtral-12B.  

---

**[Reasoning LLM deployment](#)**  
Deploy models with reasoning capabilities designed for long-context tasks, coding, or tool use, such as QwQ-32B.  

---

**[Hybrid Thinking LLM deployment](#)**  
Deploy models that can switch between reasoning and non-reasoning modes for flexible usage, such as Qwen-3.

```{toctree}
:hidden:

notebooks/small-size-llm.ipynb
notebooks/medium-size-llm.ipynb
```

