Ray Serve LLM API
==============================


.. currentmodule:: ray.serve.llm



Builders
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   build_vllm_deployment
   build_openai_app

Configs
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   LLMConfig
   LLMServingArgs
   ModelLoadingConfig
   S3MirrorConfig
   S3AWSCredentials
   GCSMirrorConfig
   LoraConfig


Deployments
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   VLLMDeploymentImpl
   LLMModelRouterDeploymentImpl


