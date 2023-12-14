(serve-triton-server-integration)=

# Serving a Resenet model with Triton Server
This guide shows how to serve models with [NVIDIA Triton Server](https://github.com/triton-inference-server/server) using Ray Serve.

## Installation
Here is the Dockerfile example for installing Triton Server with Ray Serve.

## Inference Code
Before starting the triton serve to do the inference, you need to place your model into the model repository (e.g. `/tmp/models`). So that the triton serve can get access to your models.
You can follow the [guide][https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_repository.md#model-repository] to setup the model repo.

In the example, the model repo is under `/tmp/models` and the model name is `resnet50_libtorch`.

```bash
ray@ip-10-0-60-123:~/default/tutorials/RayServe/models_test$ ls /tmp/models/resnet50_libtorch/
1  config.pbtxt  resnet50_labels.txt
```

Here is the inference code example for serving a model with Triton Server.

```python
import numpy
import requests
from fastapi import FastAPI
from ray import serve
from tritonserver_api import TritonServer


# 1: Define a FastAPI app and wrap it in a deployment with a route handler.
app = FastAPI()


@serve.deployment(route_prefix="/", ray_actor_options={"num_gpus": 1}, autoscaling_config={"min_replicas": 0, "max_replicas": 2})
@serve.ingress(app)
class TritonDeployment:
    def __init__(self):
        # Construct the triton server
        server_options = TritonServer.Options("<model_repo_path>")
        self._triton_server = TritonServer(server_options)
        self._triton_server.start()

    @app.get("/classify")
    def classify(self) -> float:
        model = self._triton_server.model("resnet50_libtorch")

        input_ = numpy.random.rand(1, 3, 224, 224).astype(numpy.float32)

        responses = model.infer_async(inputs={"INPUT__0": input_})
        for response in responses:
            output_ = response.outputs["OUTPUT__0"]
            max_ = numpy.argmax(output_[0])
        return max_


def do_request():
    requests.get(
        "http://localhost:8000/classify",
    ).raise_for_status()

if __name__ == "__main__":
    # 2: Deploy the deployment.
    serve.run(TritonDeployment.bind())
    [do_request() for _ in range(10)]

else:
    app = TritonDeployment.bind()
```

Save the above code to a file named e.g. `triton_serve.py`, then run `python triton_serve.py` to start the server and send classify requests. 

If you find any bugs or have any suggestions, please let us know by [filing an issue](https://github.com/ray-project/ray/issues) on GitHub.
