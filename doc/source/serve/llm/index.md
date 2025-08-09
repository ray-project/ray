(serving-llms)=

# Serving LLMs

Ray Serve LLM APIs allow users to deploy multiple LLM models together with a familiar Ray Serve API, while providing compatibility with the OpenAI API.

## Features

- ⚡️ Automatic scaling and load balancing
- 🌐 Unified multi-node multi-model deployment
- 🔌 OpenAI compatible
- 🔄 Multi-LoRA support with shared base models
- 🚀 Engine agnostic architecture (i.e. vLLM, SGLang, etc)

## Requirements

```bash
pip install ray[serve,llm]>=2.43.0 vllm>=0.7.2

# Suggested dependencies when using vllm 0.7.2:
pip install xgrammar==0.1.11 pynvml==12.0.0
```

## Key Components

The ray.serve.llm module provides two key deployment types for serving LLMs:

### LLMServer

The LLMServer sets up and manages the vLLM engine for model serving. It can be used standalone or combined with your own custom Ray Serve deployments.

### OpenAiIngress

This deployment provides an OpenAI-compatible FastAPI ingress and routes traffic to the appropriate model for multi-model services. The following endpoints are supported:

- `/v1/chat/completions`: Chat interface (ChatGPT-style)
- `/v1/completions`: Text completion
- `/v1/embeddings`: Text embeddings
- `/v1/models`: List available models
- `/v1/models/{model}`: Model information

## Configuration

### LLMConfig

The LLMConfig class specifies model details such as:

- Model loading sources (HuggingFace or cloud storage)
- Hardware requirements (accelerator type)
- Engine arguments (e.g. vLLM engine kwargs)
- LoRA multiplexing configuration
- Serve auto-scaling parameters

```{toctree}
:hidden:

Quickstart <quick-start>
Prefill/Decode Disaggregation <pd-dissagregation>
```


