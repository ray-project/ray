.. _serving_llms:

Serving LLMs
============

Ray Serve LLM APIs allow users to deploy multiple LLM models together with a familiar Ray Serve API, while providing compatibility with the OpenAI API.

Features
--------
- ⚡️ Automatic scaling and load balancing
- 🌐 Unified multi-node multi-model deployment
- 🔌 OpenAI compatible
- 🔄 Multi-LoRA support with shared base models
- 🚀 Engine agnostic architecture (i.e. vLLM, SGLang, etc)

Requirements
--------------

.. code-block:: bash

    pip install ray[serve,llm]>=2.43.0 vllm>=0.7.2

    # Suggested dependencies when using vllm 0.7.2:
    pip install xgrammar==0.1.11 pynvml==12.0.0


Key Components
--------------

The ``ray.serve.llm`` module provides two key deployment types for serving LLMs:

LLMServer
~~~~~~~~~~~~~~~~~~

The LLMServer sets up and manages the vLLM engine for model serving. It can be used standalone or combined with your own custom Ray Serve deployments.

LLMRouter
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



Deployment through ``LLMRouter``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from ray import serve
    from ray.serve.llm import LLMConfig, LLMServer, LLMRouter

    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id="qwen-0.5b",
            model_source="Qwen/Qwen2.5-0.5B-Instruct",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1, max_replicas=2,
            )
        ),
        # Pass the desired accelerator type (e.g. A10G, L4, etc.)
        accelerator_type="A10G",
        # You can customize the engine arguments (e.g. vLLM engine kwargs)
        engine_kwargs=dict(
            tensor_parallel_size=2,
        ),
    )

    # Deploy the application
    deployment = LLMServer.as_deployment(llm_config.get_serve_options(name_prefix="vLLM:")).bind(llm_config)
    llm_app = LLMRouter.as_deployment().bind([deployment])
    serve.run(llm_app)

You can query the deployed models using either cURL or the OpenAI Python client:

.. tab-set::

    .. tab-item:: cURL
        :sync: curl

        .. code-block:: bash

            curl -X POST http://localhost:8000/v1/chat/completions \
                 -H "Content-Type: application/json" \
                 -H "Authorization: Bearer fake-key" \
                 -d '{
                       "model": "qwen-0.5b",
                       "messages": [{"role": "user", "content": "Hello!"}]
                     }'

    .. tab-item:: Python
        :sync: python

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

            # Basic chat completion with streaming
            response = client.chat.completions.create(
                model="qwen-0.5b",
                messages=[{"role": "user", "content": "Hello!"}],
                stream=True
            )

            for chunk in response:
                if chunk.choices[0].delta.content is not None:
                    print(chunk.choices[0].delta.content, end="", flush=True)


For deploying multiple models, you can pass a list of ``LLMConfig`` objects to the ``LLMRouter`` deployment:

.. code-block:: python

    from ray import serve
    from ray.serve.llm import LLMConfig, LLMServer, LLMRouter

    llm_config1 = LLMConfig(
        model_loading_config=dict(
            model_id="qwen-0.5b",
            model_source="Qwen/Qwen2.5-0.5B-Instruct",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1, max_replicas=2,
            )
        ),
        accelerator_type="A10G",
    )

    llm_config2 = LLMConfig(
        model_loading_config=dict(
            model_id="qwen-1.5b",
            model_source="Qwen/Qwen2.5-1.5B-Instruct",
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1, max_replicas=2,
            )
        ),
        accelerator_type="A10G",
    )

    # Deploy the application
    deployment1 = LLMServer.as_deployment(llm_config1.get_serve_options(name_prefix="vLLM:")).bind(llm_config1)
    deployment2 = LLMServer.as_deployment(llm_config2.get_serve_options(name_prefix="vLLM:")).bind(llm_config2)
    llm_app = LLMRouter.as_deployment().bind([deployment1, deployment2])
    serve.run(llm_app)


Production Deployment
---------------------

For production deployments, Ray Serve LLM provides utilities for config-driven deployments. You can specify your deployment configuration using YAML files:

.. tab-set::

    .. tab-item:: Inline Config
        :sync: inline

        .. code-block:: yaml

            # config.yaml
            applications:
            - args:
                llm_configs:
                    - model_loading_config:
                        model_id: qwen-0.5b
                        model_source: Qwen/Qwen2.5-0.5B-Instruct
                      accelerator_type: A10G
                      deployment_config:
                        autoscaling_config:
                            min_replicas: 1
                            max_replicas: 2
                    - model_loading_config:
                        model_id: qwen-1.5b
                        model_source: Qwen/Qwen2.5-1.5B-Instruct
                      accelerator_type: A10G
                      deployment_config:
                        autoscaling_config:
                            min_replicas: 1
                            max_replicas: 2
              import_path: ray.serve.llm:build_openai_app
              name: llm_app
              route_prefix: "/"


    .. tab-item:: Standalone Config
        :sync: standalone

        .. code-block:: yaml

            # config.yaml
            applications:
            - args:
                llm_configs:
                    - models/qwen-0.5b.yaml
                    - models/qwen-1.5b.yaml
              import_path: ray.serve.llm:build_openai_app
              name: llm_app
              route_prefix: "/"


        .. code-block:: yaml

            # models/qwen-0.5b.yaml
            model_loading_config:
              model_id: qwen-0.5b
              model_source: Qwen/Qwen2.5-0.5B-Instruct
            accelerator_type: A10G
            deployment_config:
              autoscaling_config:
                min_replicas: 1
                max_replicas: 2

        .. code-block:: yaml

            # models/qwen-1.5b.yaml
            model_loading_config:
              model_id: qwen-1.5b
              model_source: Qwen/Qwen2.5-1.5B-Instruct
            accelerator_type: A10G
            deployment_config:
              autoscaling_config:
                min_replicas: 1
                max_replicas: 2

To deploy using either configuration file:

.. code-block:: bash

    serve run config.yaml

Advanced Usage Patterns
-----------------------

For each usage pattern, we provide a server and client code snippet.

Multi-LoRA Deployment
~~~~~~~~~~~~~~~~~~~~~

You can use LoRA (Low-Rank Adaptation) to efficiently fine-tune models by configuring the ``LoraConfig``.
We use Ray Serve's multiplexing feature to serve multiple LoRA checkpoints from the same model.
This allows the weights to be loaded on each replica on-the-fly and be cached via an LRU mechanism.

.. tab-set::

    .. tab-item:: Server
        :sync: server

        .. code-block:: python

            from ray import serve
            from ray.serve.llm import LLMConfig, build_openai_app

            # Configure the model with LoRA
            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-0.5b",
                    model_source="Qwen/Qwen2.5-0.5B-Instruct",
                ),
                lora_config=dict(
                    # Let's pretend this is where LoRA weights are stored on S3.
                    # For example
                    # s3://my_dynamic_lora_path/lora_model_1_ckpt
                    # s3://my_dynamic_lora_path/lora_model_2_ckpt
                    # are two of the LoRA checkpoints
                    dynamic_lora_loading_path="s3://my_dynamic_lora_path",
                    max_num_adapters_per_replica=16,
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            # Build and deploy the model
            app = build_openai_app({"llm_configs": [llm_config]})
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

            # Make a request to the desired lora checkpoint
            response = client.chat.completions.create(
                model="qwen-0.5b:lora_model_1_ckpt",
                messages=[{"role": "user", "content": "Hello!"}],
                stream=True,
            )

            for chunk in response:
                if chunk.choices[0].delta.content is not None:
                    print(chunk.choices[0].delta.content, end="", flush=True)


Structured Output
~~~~~~~~~~~~~~~~~

For structured output, you can use JSON mode similar to OpenAI's API:

.. tab-set::

    .. tab-item:: Server
        :sync: server

        .. code-block:: python

            from ray import serve
            from ray.serve.llm import LLMConfig, build_openai_app

            # Configure the model with LoRA
            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-0.5b",
                    model_source="Qwen/Qwen2.5-0.5B-Instruct",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
                accelerator_type="A10G",
            )

            # Build and deploy the model
            app = build_openai_app({"llm_configs": [llm_config]})
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python


            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

            # Request structured JSON output
            response = client.chat.completions.create(
                model="qwen-0.5b",
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
                ],
                stream=True,
            )

            for chunk in response:
                if chunk.choices[0].delta.content is not None:
                    print(chunk.choices[0].delta.content, end="", flush=True)
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
            from ray.serve.llm import LLMConfig, build_openai_app


            # Configure a vision model
            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="pixtral-12b",
                    model_source="mistral-community/pixtral-12b",
                ),
                deployment_config=dict(
                    autoscaling_config=dict(
                        min_replicas=1,
                        max_replicas=2,
                    )
                ),
                accelerator_type="L40S",
                engine_kwargs=dict(
                    tensor_parallel_size=1,
                    max_model_len=8192,
                ),
            )

            # Build and deploy the model
            app = build_openai_app({"llm_configs": [llm_config]})
            serve.run(app)

    .. tab-item:: Client
        :sync: client

        .. code-block:: python

            from openai import OpenAI

            # Initialize client
            client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

            # Create and send a request with an image
            response = client.chat.completions.create(
                model="pixtral-12b",
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
                ],
                stream=True,
            )

            for chunk in response:
                if chunk.choices[0].delta.content is not None:
                    print(chunk.choices[0].delta.content, end="", flush=True)

Frequently Asked Questions
--------------------------

How do I use gated Huggingface models?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use `runtime_env` to specify the env variables that are required to access the model.
To set the deployment options, you can use the ``get_serve_options`` method on the ``LLMConfig`` object.

.. code-block:: python

    from ray import serve
    from ray.serve.llm import LLMConfig, LLMServer, LLMRouter
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
        # Pass the desired accelerator type (e.g. A10G, L4, etc.)
        accelerator_type="A10G",
        runtime_env=dict(
            env_vars=dict(
                HF_TOKEN=os.environ["HF_TOKEN"]
            )
        ),
    )

    # Deploy the application
    deployment = LLMServer.as_deployment(llm_config.get_serve_options(name_prefix="vLLM:")).bind(llm_config)
    llm_app = LLMRouter.as_deployment().bind([deployment])
    serve.run(llm_app)

Why is downloading the model so slow?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are using huggingface models, you can enable fast download by setting `HF_HUB_ENABLE_HF_TRANSFER` and installing `pip install hf_transfer`.



.. code-block:: python

    from ray import serve
    from ray.serve.llm import LLMConfig, LLMServer, LLMRouter
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
        # Pass the desired accelerator type (e.g. A10G, L4, etc.)
        accelerator_type="A10G",
        runtime_env=dict(
            env_vars=dict(
                HF_TOKEN=os.environ["HF_TOKEN"],
                HF_HUB_ENABLE_HF_TRANSFER="1"
            )
        ),
    )

    # Deploy the application
    deployment = LLMServer.as_deployment(llm_config.get_serve_options(name_prefix="vLLM:")).bind(llm_config)
    llm_app = LLMRouter.as_deployment().bind([deployment])
    serve.run(llm_app)

Usage Data Collection
--------------------------
We collect usage data to improve Ray Serve LLM.
We collect data about the following features and attributes:

- model architecture used for serving
- whether JSON mode is used
- whether LoRA is used and how many LoRA weights are loaded initially at deployment time
- whether autoscaling is used and the min and max replicas setup
- tensor parallel size used
- initial replicas count
- GPU type used and number of GPUs used

If you would like to opt-out from usage data collection, you can follow :ref:`Ray usage stats <ref-usage-stats>`
to disable it.
