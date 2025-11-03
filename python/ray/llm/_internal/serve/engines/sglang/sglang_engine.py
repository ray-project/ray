import ray
import requests
from fastapi import FastAPI
from ray import serve
from ray.serve.handle import DeploymentHandle

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class MyFastAPIDeployment:
    def __init__(self, engine_handle: DeploymentHandle):
        self._engine_handle = engine_handle

    @app.get("/v1/chat/completions/{in_prompt}") # serve as OPENAI /v1/chat/completions endpoint 
    async def root(self, in_prompt: str):                 # make endpoint async
        # Asynchronous call to a method of that deployment (executes remotely) used remote
        res = await self._engine_handle.chat.remote(in_prompt) 
        return "useing Llama-3.1-8B-Instruct for your !", in_prompt, res

@serve.deployment
class SGLangServer:
    def __init__(self):

        self.engine_kwargs = dict(
            model_path = "/scratch2/huggingface/hub/meta-llama/Llama-3.1-8B-Instruct/",
            mem_fraction_static = 0.8,
            tp_size = 8,
        )

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

sglangServer = SGLangServer.bind()
my_App = MyFastAPIDeployment.bind(sglangServer)
handle: DeploymentHandle = serve.run(my_App, blocking = True)

