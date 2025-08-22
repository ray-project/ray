# Quick-starts with LLM Serving

These guides provide a fast path to serving LLMs using Ray Serve on Anyscale, with focused tutorials for different deployment scales, from single-GPU setups to multi-node clusters.

Each tutorial includes development and production setups, tips for configuring your cluster, and guidance on monitoring and scaling with Ray Serve.

## Tutorial Categories

**[Small size LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/small-size-llm/notebook.html)**  
Deploy small size models on a single GPU, such as Llama 3 8&nbsp;B, Mistral 7&nbsp;B, or Phi-2.  

---

**[Medium size LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/medium-size-llm/notebook.html)**  
Deploy medium size models using tensor parallelism across 4—8 GPUs on a single node, such as Llama 3 70&nbsp;B, Qwen 14&nbsp;B, Mixtral 8x7&nbsp;B.  

---

**[Large size LLM deployment](https://github.com/anyscale/templates/tree/main/templates/ray_serve_llm/end-to-end-examples/gargantuan_model)**  
Deploy massive models using pipeline parallelism across a multi-node cluster, such as Deepseek-R1 or Llama-Nemotron-253&nbsp;B.  

---

**[Vision LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/vision-llm/notebook.html)**  
Deploy models with image + text input such as Qwen 2.5-VL-7&nbsp;B-Instruct, MiniGPT-4, or Pixtral-12&nbsp;B.  

---

**[Reasoning LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/reasoning-llm/notebook.html)**  
Deploy models with reasoning capabilities designed for long-context tasks, coding, or tool use, such as QwQ-32&nbsp;B.  

---

**[Hybrid Thinking LLM deployment](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/notebooks/hybrid-reasoning-llm.html)**  
Deploy models that can switch between reasoning and non-reasoning modes for flexible usage, such as Qwen-3.

```{toctree}
:hidden:

small-size-llm/README.ipynb
medium-size-llm/README.ipynb
vision-llm/README.ipynb
reasoning-llm/README.ipynb
hybrid-reasoning-llm/README.ipynb
```

