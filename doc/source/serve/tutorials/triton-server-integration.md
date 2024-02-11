(serve-triton-server-integration)=

# Serving models with Triton Server in Ray Serve
This guide shows how to build an application with stable diffusion model using [NVIDIA Triton Server](https://github.com/triton-inference-server/server) in Ray Serve.

## Preparation

### Installation
It is recommended to use the `nvcr.io/nvidia/tritonserver:23.12-py3` image which already has the Triton Server python API library installed, and install the ray serve lib by `pip install "ray[serve]"` inside the image.

### Build and export a model
For this application, the encoder is exported to ONNX format and the stable diffusion model is exported to be TensorRT engine format which is being compatible with Triton Server.
Here is the example to export models to be in ONNX format.([source](https://github.com/triton-inference-server/tutorials/blob/main/Triton_Inference_Server_Python_API/scripts/stable_diffusion/export.py))

```python
import torch
from diffusers import AutoencoderKL
from transformers import CLIPTextModel, CLIPTokenizer

prompt = "Draw a dog"
vae = AutoencoderKL.from_pretrained(
    "CompVis/stable-diffusion-v1-4", subfolder="vae", use_auth_token=True
)

tokenizer = CLIPTokenizer.from_pretrained("openai/clip-vit-large-patch14")
text_encoder = CLIPTextModel.from_pretrained("openai/clip-vit-large-patch14")

vae.forward = vae.decode
torch.onnx.export(
    vae,
    (torch.randn(1, 4, 64, 64), False),
    "vae.onnx",
    input_names=["latent_sample", "return_dict"],
    output_names=["sample"],
    dynamic_axes={
        "latent_sample": {0: "batch", 1: "channels", 2: "height", 3: "width"},
    },
    do_constant_folding=True,
    opset_version=14,
)

text_input = tokenizer(
    prompt,
    padding="max_length",
    max_length=tokenizer.model_max_length,
    truncation=True,
    return_tensors="pt",
)

torch.onnx.export(
    text_encoder,
    (text_input.input_ids.to(torch.int32)),
    "encoder.onnx",
    input_names=["input_ids"],
    output_names=["last_hidden_state", "pooler_output"],
    dynamic_axes={
        "input_ids": {0: "batch", 1: "sequence"},
    },
    opset_version=14,
    do_constant_folding=True,
)
```

From the script, the outputs are `vae.onnx` and `encoder.onnx`.

After the ONNX model exported, convert the ONNX model to the TensorRT engine serialized file. ([Details](https://github.com/NVIDIA/TensorRT/blob/release/9.2/samples/trtexec/README.md?plain=1#L22) about trtexec cli)
```bash
trtexec --onnx=vae.onnx --saveEngine=vae.plan --minShapes=latent_sample:1x4x64x64 --optShapes=latent_sample:4x4x64x64 --maxShapes=latent_sample:8x4x64x64 --fp16
```

### Prepare the model repository
Triton Server requires a [model repository](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_repository.md) to store the models, which is a local directory or remote blob store (e.g. AWS S3) containing the model configuration and the model files.
In our example, we will use a local directory as the model repository to save all the model files.

```bash
model_repo/
├── stable_diffusion
│   ├── 1
│   │   └── model.py
│   └── config.pbtxt
├── text_encoder
│   ├── 1
│   │   └── model.onnx
│   └── config.pbtxt
└── vae
    ├── 1
    │   └── model.plan
    └── config.pbtxt
```

The model repository contains three models: `stable_diffusion`, `text_encoder` and `vae`. Each model has a `config.pbtxt` file and a model file. The `config.pbtxt` file contains the model configuration, which is used to describe the model type and input/output formats.(you can learn more about model config file [here](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_configuration.md)). To get config files for our example, you can download them from [here](https://github.com/triton-inference-server/tutorials/tree/main/Conceptual_Guide/Part_6-building_complex_pipelines/model_repository). We use `1` as the version of each model. The model files are saved in the version directory.


## Start the Triton Server inside a Ray Serve application
In each serve replica, there is a single Triton Server instance running. The API takes the model repository path as the parameter, and the Triton Serve instance is started during the replica initialization. The models can be loaded during the inference requests, and the loaded models are cached in the Triton Server instance.

Here is the inference code example for serving a model with Triton Server.([source](https://github.com/triton-inference-server/tutorials/blob/main/Triton_Inference_Server_Python_API/examples/rayserve/tritonserver_deployment.py))

```python
import numpy
import requests
import tritonserver
from fastapi import FastAPI
from PIL import Image
from ray import serve


app = FastAPI()

@serve.deployment(ray_actor_options={"num_gpus": 1})
@serve.ingress(app)
class TritonDeployment:
    def __init__(self):
        self._triton_server = tritonserver

        model_repository = ["/workspace/models"]

        self._triton_server = tritonserver.Server(
            model_repository=model_repository,
            model_control_mode=tritonserver.ModelControlMode.EXPLICIT,
            log_info=False,
        )
        self._triton_server.start(wait_until_ready=True)

    @app.get("/generate")
    def generate(self, prompt: str, filename: str = "generated_image.jpg") -> None:
        if not self._triton_server.model("stable_diffusion").ready():
            try:
                self._triton_server.load("text_encoder")
                self._triton_server.load("vae")
                self._stable_diffusion = self._triton_server.load("stable_diffusion")
                if not self._stable_diffusion.ready():
                    raise Exception("Model not ready")
            except Exception as error:
                print(f"Error can't load stable diffusion model, {error}")
                return

        for response in self._stable_diffusion.infer(inputs={"prompt": [[prompt]]}):
            generated_image = (
                numpy.from_dlpack(response.outputs["generated_image"])
                .squeeze()
                .astype(numpy.uint8)
            )

            image_ = Image.fromarray(generated_image)
            image_.save(filename)


if __name__ == "__main__":
    # Deploy the deployment.
    serve.run(TritonDeployment.bind())

    # Query the deployment.
    requests.get(
        "http://localhost:8000/generate",
        params={"prompt": "dogs in new york, realistic, 4k, photograph"},
    )
```

Save the above code to a file named e.g. `triton_serve.py`, then run `python triton_serve.py` to start the server and send classify requests. After you run the above code, you should see the image generated `generated_image.jpg`. Check it out!
![image](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/triton_server_stable_diffusion.jpg)


:::{note}
You can also use remote model repository, such as AWS S3, to store the model files. To use remote model repository, you need to set the `model_repository` variable to the remote model repository path.  For example `model_repository = s3://<bucket_name>/<model_repository_path>`.
:::

If you find any bugs or have any suggestions, please let us know by [filing an issue](https://github.com/ray-project/ray/issues) on GitHub.
