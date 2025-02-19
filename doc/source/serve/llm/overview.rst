Overview
========

Ray Serve LLM APIs allow users to deploy multiple LLM models together with a familiar Ray Serve API, while providing compatibility with the OpenAI API.

Features
--------
- ⚡️ Automatic scaling and load balancing
- 🌐 Unified multi-node multi-model deployment
- 🔌 OpenAI compatible
- 🔄 Multi-LoRA support with shared base models
- 🚀 Engine agnostic architecture (i.e. vLLM, SGLang, etc)

Key Components
--------------

The ``ray.serve.llm`` module provides two key deployment types for serving LLMs:

VLLMDeploymentImpl
~~~~~~~~~~~~~~~~~~

The VLLMDeploymentImpl sets up and manages the vLLM engine for model serving. It can be used standalone or combined with your own custom Ray Serve deployments.

LLMModelRouterDeploymentImpl
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This deployment provides an OpenAI-compatible FastAPI ingress and routes traffic to the appropriate model for multi-model services. The following endpoints are supported:

- ``/v1/chat/completions``: Chat interface (ChatGPT-style)
- ``/v1/completions``: Text completion
- ``/v1/models``: List available models
- ``/v1/models/{model}``: Model information

Configuration
-------------

LLMConfig
~~~~~~~~~
The ``LLMConfig`` class specifies model details such as:

- Model loading sources (HuggingFace or cloud storage)
- Hardware requirements (accelerator type)
- Engine arguments (e.g. vLLM engine kwargs)
- LoRA multiplexing configuration
- Serve auto-scaling parameters

Quickstart Examples
-------------------

Single Model Deployment through ``VLLMDeploymentImpl``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from ray import serve
    from ray.serve.config import AutoscalingConfig
    from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
    from ray.serve.llm.deployments import VLLMDeploymentImpl
    from ray.serve.llm.openai_api_models import ChatCompletionRequest

    # Configure the model
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            served_model_name="llama-3.1-8b",
            model_source="meta-llama/Llama-3.1-8b-instruct",
        ),
        deployment_config=DeploymentConfig(
            autoscaling_config=AutoscalingConfig(
                min_replicas=1,
                max_replicas=2,
            )
        ),
    )

    # Build the deployment directly
    VLLMDeploymentImpl = VLLMDeploymentImpl.as_deployment()
    vllm_app = VLLMDeploymentImpl.options(**llm_config.get_serve_options()).bind(llm_config)

    model_handle = serve.run(vllm_app)

Multi-Model Deployment through ``LLMModelRouterDeploymentImpl``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from ray import serve
    from ray.serve.config import AutoscalingConfig
    from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
    from ray.serve.llm.deployments import VLLMDeploymentImpl
    from ray.serve.llm.openai_api_models import ChatCompletionRequest

    llm_config1 = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            served_model_name="llama-3.1-8b",  # Name shown in /v1/models
            model_source="meta-llama/Llama-3.1-8b-instruct",
        ),
        deployment_config=DeploymentConfig(
            autoscaling_config=AutoscalingConfig(
                min_replicas=1, max_replicas=8,
            )
        ),
    )
    llm_config2 = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            served_model_name="llama-3.2-3b",  # Name shown in /v1/models
            model_source="meta-llama/Llama-3.2-3b-instruct",
        ),
        deployment_config=DeploymentConfig(
            autoscaling_config=AutoscalingConfig(
                min_replicas=1, max_replicas=8,
            )
        ),
    )

    # Deploy the application
    vllm_deployment1 = VLLMDeploymentImpl.as_deployment(llm_config1.get_serve_options()).bind(llm_config1)
    vllm_deployment2 = VLLMDeploymentImpl.as_deployment(llm_config2.get_serve_options()).bind(llm_config2)
    llm_app = LLMModelRouterDeploymentImpl.as_deployment().bind([vllm_deployment1, vllm_deployment2])
    serve.run(llm_app)

Querying Models
---------------

You can query the deployed models using either cURL or the OpenAI Python client:

.. tab-set::

    .. tab-item:: cURL
        :sync: curl

        .. code-block:: bash

            curl -X POST http://localhost:8000/v1/chat/completions \
                 -H "Content-Type: application/json" \
                 -H "Authorization: Bearer fake-key" \
                 -d '{
                       "model": "llama-3.2-3b",
                       "messages": [{"role": "user", "content": "Hello!"}]
                     }'

    .. tab-item:: Python
        :sync: python

        .. code-block:: python

            # Query
            from openai import OpenAI
            
            # Initialize client
            client = OpenAI(base_url="http://localhost:8000", api_key="fake-key")
            
            # Basic completion
            response = client.chat.completions.create(
                model="llama-3.2-3b",
                messages=[{"role": "user", "content": "Hello!"}]
            )

Production Deployment
---------------------

For production deployments, Ray Serve LLM provides utilities for config-driven deployments. You can specify your deployment configuration using YAML files:

.. tab-set::

    .. tab-item:: Inline Config
        :sync: inline

        .. code-block:: yaml

            # config.yaml
            application:
              name: llm_app
              route_prefix: "/"
              import_path: ray.serve.llm.builders:build_openai_app
              args: 
                llm_configs:
                - model_loading_config:
                    model_id: meta-llama/Meta-Llama-3.1-8B-Instruct
                  accelerator_type: A10G
                  tensor_parallelism:
                    degree: 1
                  deployment_config:
                    autoscaling_config: 
                      min_replicas: 1
                      max_replicas: 2

    .. tab-item:: Standalone Config
        :sync: standalone

        .. code-block:: yaml

            # config.yaml
            application:
              name: llm_app
              route_prefix: "/"
              import_path: ray.serve.llm.builders:build_openai_app
              args: 
                - examples/llama-3.1-8b.yaml

        .. code-block:: yaml

            # examples/llama-3.1-8b.yaml
            model_loading_config:
              model_id: meta-llama/Meta-Llama-3.1-8B-Instruct
            accelerator_type: A10G
            tensor_parallelism:
              degree: 1
            deployment_config:
              autoscaling_config: 
                min_replicas: 1
                max_replicas: 2

To deploy using either configuration file:

.. code-block:: bash

    serve run config.yaml

Advanced Usage Patterns
-----------------------

Multi-LoRA Deployment
~~~~~~~~~~~~~~~~~~~~~

You can use LoRA (Low-Rank Adaptation) to efficiently fine-tune models by configuring the ``LoraConfig``:

.. tab-set::

    .. tab-item:: Server
        :sync: server

        .. code-block:: python

            from ray import serve
            from ray.serve.config import AutoscalingConfig
            from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, LoraConfig, DeploymentConfig
            from ray.serve.llm.builders import build_openai_app

            # Configure the model with LoRA
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                lora_config=LoraConfig(
                    # Let's pretend this is where LoRA weights are stored on S3.
                    # For example
                    # s3://ray-serve-llm-lora/llama-3.1-8b-instruct-lora/llama-3.1-8b:abc/
                    # and s3://ray-serve-llm-lora/llama-3.1-8b-instruct-lora/llama-3.1-8b:def/
                    # are two of the LoRA checkpoints (i.e. in form of <base_model>:<lora_id>)
                    dynamic_lora_loading_path="s3://ray-serve-llm-lora/llama-3.1-8b-instruct-lora",
                    max_num_adapters_per_replica=16,
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
            )

            # Build and deploy the model
            app = build_openai_app([llm_config])
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000", api_key="fake-key")

            # Make a request to the desired lora checkpoint
            response = client.chat.completions.create(
                model="llama-3.1-8b:abc",
                messages=[{"role": "user", "content": "Hello!"}]
            )

Structured Output
~~~~~~~~~~~~~~~~~

For structured output, you can use JSON mode similar to OpenAI's API:

.. tab-set::

    .. tab-item:: Server
        :sync: server

        .. code-block:: python

            from ray import serve
            from ray.serve.config import AutoscalingConfig
            from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
            from ray.serve.llm.builders import build_openai_app

            # Configure and deploy the model
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llama-3.1-8b",
                    model_source="meta-llama/Llama-3.1-8b-instruct",
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
            )

            # Build and deploy the model
            app = build_openai_app([llm_config])
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000", api_key="fake-key")

            # Request structured JSON output
            response = client.chat.completions.create(
                model="llama-3.1-8b",
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that outputs JSON."
                    },
                    {
                        "role": "user",
                        "content": "List three colors in JSON format"
                    }
                ]
            )
            # Example response:
            # {
            #   "colors": [
            #     "red",
            #     "blue",
            #     "green"
            #   ]
            # }

Vision Language Models
~~~~~~~~~~~~~~~~~~~~~~

For multimodal models that can process both text and images:

.. tab-set::

    .. tab-item:: Server
        :sync: server

        .. code-block:: python

            from ray import serve
            from ray.serve.config import AutoscalingConfig
            from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig, DeploymentConfig
            from ray.serve.llm.builders import build_openai_app

            # Configure a vision model
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    served_model_name="llava-1.5-13b",
                    model_source="liuhaotian/llava-v1.5-13b",
                ),
                deployment_config=DeploymentConfig(
                    autoscaling_config=AutoscalingConfig(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
            )

            # Build and deploy the model
            app = build_openai_app([llm_config])
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000", api_key="fake-key")

            # Create and send a request with an image
            response = client.chat.completions.create(
                model="llava-1.5-13b",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "What's in this image?"
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": "https://example.com/image.jpg"
                                }
                            }
                        ]
                    }
                ]
            )
