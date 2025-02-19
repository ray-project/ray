Ray Serve LLM API
==============================


.. currentmodule:: ray.serve.llm



Builders
---------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

   builders.build_vllm_deployment
   builders.build_openai_app

Configs
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   configs.LLMConfig
   configs.LLMServingArgs
   configs.ModelLoadingConfig
   configs.S3MirrorConfig
   configs.S3AWSCredentials
   configs.GCSMirrorConfig
   configs.LoraConfig
   .. TODO: Remove this once we have a better way to handle the deployment config
   
   configs.DeploymentConfig  
   configs.AutoscalingConfig

