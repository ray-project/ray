<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Quickstarts for LLM serving

These guides provide a fast path to serving LLMs using Ray Serve on Anyscale, with focused tutorials for different deployment scales, from single-GPU setups to multi-node clusters.

Each tutorial includes development and production setups, tips for configuring your cluster, and guidance on monitoring and scaling with Ray Serve.

---

## Why use Ray Serve for LLM serving?

Ray Serve LLM provides production-grade features beyond what standalone vLLM offers:

**Horizontal scaling**: Replicate your model across multiple GPUs or nodes and automatically balance traffic across replicas. As request volume grows, Ray Serve automatically adds more replicas to handle the load.

**Production readiness**: Ray Serve provides built-in autoscaling, fault tolerance, rolling updates, and comprehensive monitoring through Grafana dashboards. The system handles replica failures gracefully and scales based on traffic patterns.

**Multi-model serving**: Deploy multiple models with different configurations on the same cluster. Each model can have its own autoscaling policy and resource requirements.

**Modular architecture**: Separate your application logic from infrastructure concerns. You can customize request routing, add authentication layers, or integrate with existing systems without modifying your model serving code.

For simple single-GPU deployments or experimentation, standalone vLLM might be sufficient. However, for production workloads that need to scale, handle failures, or serve multiple models efficiently, Ray Serve provides the infrastructure you need.

---

## Understanding the Ray Serve LLM architecture

Ray Serve LLM is built on two main components that work together to serve your model:

**LLMServer**: A Ray Serve deployment that manages a vLLM engine instance. Each replica of this deployment:

- Manages a single vLLM engine instance.
- Handles GPU placement through Ray's placement groups.
- Processes inference requests with continuous batching.
- Exposes engine metrics for monitoring.

**OpenAiIngress**: A FastAPI-based ingress deployment that:

- Provides OpenAI-compatible API endpoints (`/v1/chat/completions`, etc.).
- Routes requests to the appropriate LLMServer replicas.
- Handles load balancing across multiple replicas.
- Manages model multiplexing (for example, LoRA adapters).

When you call `build_openai_app`, Ray Serve LLM creates both components and connects them automatically. The ingress receives HTTP requests and forwards them to available LLMServer replicas through deployment handles. This architecture enables:

- **Independent scaling**: Scale the ingress and LLMServer independently based on CPU and GPU utilization.
- **Fault tolerance**: Replica failures don't affect the entire service.
- **Flexibility**: Customize routing logic or add authentication without modifying the model serving code.

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/public-images/ray-serve-llm/diagrams/llmserver.png" width="800">

For detailed technical information, including diagrams of request flow and placement strategies, see the [Architecture overview](https://docs.ray.io/en/latest/serve/llm/architecture/overview.html).

## Tutorial categories

**[Deploy a small-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html)**  
Deploy small-sized models on a single GPU, such as Llama 3 8&nbsp;B, Mistral 7&nbsp;B, or Phi-2.  

---

**[Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html)**  
Deploy medium-sized models using tensor parallelism across 4—8 GPUs on a single node, such as Llama 3 70&nbsp;B, Qwen 14&nbsp;B, Mixtral 8x7&nbsp;B.  

---

**[Deploy a large-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/large-size-llm/README.html)**  
Deploy massive models using pipeline parallelism across a multi-node cluster, such as Deepseek-R1 or Llama-Nemotron-253&nbsp;B.  

---

**[Deploy a vision LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/vision-llm/README.html)**  
Deploy models with image and text input such as Qwen 2.5-VL-7&nbsp;B-Instruct, MiniGPT-4, or Pixtral-12&nbsp;B.  

---

**[Deploy a reasoning LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/reasoning-llm/README.html)**  
Deploy models with reasoning capabilities designed for long-context tasks, coding, or tool use, such as QwQ-32&nbsp;B.  

---

**[Deploy a hybrid reasoning LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/hybrid-reasoning-llm/README.html)**  
Deploy models that can switch between reasoning and non-reasoning modes for flexible usage, such as Qwen-3.

---

**[Deploy gpt-oss](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/gpt-oss/README.html)**  
Deploy gpt-oss reasoning models for high-reasoning, production-scale workloads, for lower latency (`gpt-oss-20b`) and high-reasoning (`gpt-oss-120b`) use cases.
