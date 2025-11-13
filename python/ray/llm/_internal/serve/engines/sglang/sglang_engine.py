import ray
import requests
from ray import serve
from ray.serve.handle import DeploymentHandle

#@serve.deployment disable serve.deployment
class SGLangServer:
    def __init__(self, llm_config: LLMConfig):

        default_engine_kwargs = dict(
            model_path = "/scratch2/huggingface/hub/meta-llama/Llama-3.1-8B-Instruct/",
            mem_fraction_static = 0.8,
            tp_size = 8,
        )

        if llm_config.engine_kwargs:
            default_engine_kwargs.update(llm_config.engine_kwargs)
        self.engine_kwargs = default_engine_kwargs

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e
        self.engine = sglang.Engine(**self.engine_kwargs)

    async def chat(self, message: str):
        print('In SGLangServer CHAT with message', message)
        res = await self.engine.async_generate(
            prompt = message,
            stream = False
        )
        return {"echo": res}

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        return {'autoscaling_config': {'min_replicas': 1, 'max_replicas': 1}, 
                'placement_group_bundles': [{'CPU': 1, 'GPU': 1, 'accelerator_type:H100': 0.001}, {'GPU': 1, 'accelerator_type:H100': 0.001}], 
                'placement_group_strategy': 'PACK', 
                'ray_actor_options': {'runtime_env': 
                                      {'worker_process_setup_hook': 'ray.llm._internal.serve._worker_process_setup_hook'}
                                      }
                }

#sglangServer = SGLangServer.bind()
#my_App = MyFastAPIDeployment.bind(sglangServer)
#handle: DeploymentHandle = serve.run(my_App, blocking = True)

