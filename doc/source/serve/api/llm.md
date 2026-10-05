---
myst:
  html_meta:
    description: "API reference for ray.serve.llm: the builders, configs, and deployment classes for serving large language models with Ray Serve."
---

(serve-llm-api)=

# LLM API

The `ray.serve.llm` module builds and configures Serve applications that serve large language models.

```{eval-rst}
.. currentmodule:: ray
```

## Builders

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.llm.build_llm_deployment
   serve.llm.build_openai_app
```

## Configs

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   serve.llm.LLMConfig
   serve.llm.LLMServingArgs
   serve.llm.ModelLoadingConfig
   serve.llm.CloudMirrorConfig
   serve.llm.LoraConfig
```

## Deployments

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.llm.LLMServer
   serve.llm.deployment.PDDecodeServer
   serve.llm.deployment.PDPrefillServer
   serve.llm.deployment.DPServer
```
