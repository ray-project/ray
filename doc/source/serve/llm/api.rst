Ray Serve LLM API
==============================


.. currentmodule:: ray.serve.llm.builders



Builders
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   build_vllm_deployment
   build_openai_app


.. currentmodule:: ray.serve.llm.configs

Configs
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   LLMConfig
   LLMServingArgs
   ModelLoadingConfig
   CloudMirrorConfig
   LoraConfig

.. currentmodule:: ray.serve.llm.deployments

Deployments
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   VLLMService
   LLMRouter


.. currentmodule:: ray.serve.llm.openai_api_models

OpenAI API Models
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic_show_json.rst

   ChatCompletionRequest
   CompletionRequest
   ChatCompletionStreamResponse
   ChatCompletionResponse
   CompletionStreamResponse
   CompletionResponse
   ErrorResponse

