# Troubleshooting

Common issues and frequently asked questions for Ray Serve LLM.

## Frequently asked questions

### How do I use gated Hugging Face models?

You can use `runtime_env` to specify the env variables that are required to access the model. To get the deployment options, you can use the `get_deployment_options` method on the {class}`LLMServer <ray.serve.llm.deployment.LLMServer>` class. Each deployment class has its own `get_deployment_options` method.

```python
from ray import serve
from ray.serve.llm import LLMConfig
from ray.serve.llm.deployment import LLMServer
from ray.serve.llm.ingress import OpenAiIngress
from ray.serve.llm.builders import build_openai_app

import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3-8b-instruct",
        model_source="meta-llama/Meta-Llama-3-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    # Pass the desired accelerator type (e.g., A10G, L4, etc.)
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars=dict(
            HF_TOKEN=os.environ["HF_TOKEN"]
        )
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

### Why is downloading the model so slow?

If you're using Hugging Face models, you can enable fast download by setting `HF_HUB_ENABLE_HF_TRANSFER` and installing `pip install hf_transfer`.

```python
from ray import serve
from ray.serve.llm import LLMConfig
from ray.serve.llm.deployment import LLMServer
from ray.serve.llm.ingress import OpenAiIngress
from ray.serve.llm.builders import build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3-8b-instruct",
        model_source="meta-llama/Meta-Llama-3-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=2,
        )
    ),
    # Pass the desired accelerator type (e.g., A10G, L4, etc.)
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars=dict(
            HF_TOKEN=os.environ["HF_TOKEN"],
            HF_HUB_ENABLE_HF_TRANSFER="1"
        )
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

### vLLM NIXL EP dependency incompatibility

:::{admonition} Known issue
Users who install Ray and vLLM directly may encounter NIXL EP incompatibility error as follows:

```text
ImportError: libcudart.so.12: cannot open shared object file: No such file or directory
```

Remove the incompatible package or ensure the installed ``nixl_ep`` package is compatible with the CUDA runtime
and vLLM build in your environment.

:::

## vLLM compatibility

Each Ray release is fully tested with a compatible vLLM version.

| Ray release | vLLM version |
| ----------- | ------------ |
| 2.56.0      | 0.22.0       |
| 2.55.0      | 0.18.0       |
| 2.54.0      | 0.15.0       |
| 2.53.0      | 0.12.0       |
| 2.52.0      | 0.11.0       |
| 2.51.0      | 0.11.0       |
| 2.50.0      | 0.10.2       |

## Get help

If you encounter issues not covered in this guide:

- [Ray GitHub Issues](https://github.com/ray-project/ray/issues) - Report bugs or request features
- [Ray Slack](https://ray-distributed.slack.com) - Get help from the community
- [Ray Discourse Forum](https://discuss.ray.io) - Ask questions and share knowledge
- [Ray LLM Office Hours](https://docs.google.com/document/d/1n3-Jw_4su8yilo9zdi5OciAduoz6H_VmdL8i9sL4f-E/edit?tab=t.e700ayqsx3v3) - Learn about new features, ask questions, and get guidance from the team
  - [Past Office Hours Recordings](https://youtube.com/playlist?list=PLzTswPQNepXl2IYF8DcV35FdCoVbeL4_6&si=ik81bljIlasYAHKN) - View recordings from previous sessions

## See also

- {doc}`Quickstart examples <quick-start>`
- {doc}`Examples <examples>`
