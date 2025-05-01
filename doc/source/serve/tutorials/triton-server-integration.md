---
orphan: true
---
# Serving models with Triton Server in Ray Serve
This guide shows how to build an application with stable diffusion model using [NVIDIA Triton Server](https://github.com/triton-inference-server/server) in Ray Serve.

## Preparation
NVIDIA Triton provides a python API for integration with Ray Serve. This can allow users to leverage inference optimizations available in Triton with the development experience and multi-model capabilities of Ray Serve. These are the steps required to prepare your model for using Triton with Ray Serve, with the TensorRT backend:
1. Set up a local or remote model repository.
2. Convert your model into the ONNX format.
3. After exporting to ONNX, convert it into a TensorRT engine serialized file.
4. Setup inference with Ray Serve with a custom image

Alternatively, you can also consider and compare other inference optimizations techniques like `torch.compile` to use with Ray Serve.
1. Set up a local or remote model repository (it's not optional, details buried deeper in the tutorial).
2. Manually convert your model into the ONNX format, regardless of what framework you originally used.
3. After exporting to ONNX, convert it into a TensorRT engine serialized file.
4. Finally, once everything is properly formatted and in place, you can load the model for serving.


## Part I: Convert and serialize the model
The encoder, unet and decoder are all converted using onnx format and serialized using TensorRT in this part.
```python
import torch
from diffusers import StableDiffusionPipeline
import os
import logging
from pathlib import Path
import sys

# Add Triton server's Python packages to the path
sys.path.append('/opt/tritonserver/python')

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create output directory
os.makedirs("model_repository/stable_diffusion/1", exist_ok=True)

# Load a specific model version that's known to work well with ONNX conversion
model_id = "runwayml/stable-diffusion-v1-5"  # This is often the most compatible
model_path = Path("model_repository/stable_diffusion/1")

pipe = StableDiffusionPipeline.from_pretrained(model_id)

# Move model to GPU if available
device = "cuda" if torch.cuda.is_available() else "cpu"
pipe = pipe.to(device)

# Get model dimensions before attempting export
# This is crucial to ensure matrix dimensions match
unet = pipe.unet
text_encoder = pipe.text_encoder
hidden_size = text_encoder.config.hidden_size

# Log dimensions for debugging
logger.info(f"Text encoder output dimension: {hidden_size}")
logger.info(f"UNet cross attention dimension: {unet.config.cross_attention_dim}")

# Only proceed if dimensions match
if hidden_size != unet.config.cross_attention_dim:
    logger.error("ERROR: Model component dimensions don't match! Cannot convert.")
    exit(1)

# Create correctly sized inputs based on actual model dimensions
batch_size = 1

# Create dummy inputs with correct dimensions
dummy_text_input = torch.ones((batch_size, 77), dtype=torch.int64, device=device)
dummy_sample = torch.randn(batch_size, 4, 64, 64, device=device)
timestep = torch.tensor([999], device=device)
encoder_hidden_states = torch.randn(batch_size, 77, hidden_size, device=device)

# Export text encoder
torch.onnx.export(
    text_encoder,
    dummy_text_input,
    str(model_path / 'text_encoder.onnx'),
    opset_version=14,
    input_names=["input_ids"],
    output_names=["last_hidden_state"],
    do_constant_folding=True,
    export_params=True,
)

logger.info("Text encoder exported successfully")

# Export UNet
torch.onnx.export(
    unet,
    (dummy_sample, timestep, encoder_hidden_states),
    str(model_path / 'unet.onnx'),
    opset_version=14,
    input_names=["sample", "timestep", "encoder_hidden_states"],
    output_names=["out_sample"],
    do_constant_folding=True,
    export_params=True,
)

logger.info("UNet exported successfully")

# Export VAE decoder
dummy_latent = torch.randn(batch_size, 4, 64, 64, device=device)

torch.onnx.export(
    pipe.vae.decoder,
    dummy_latent,
    str(model_path / 'vae_decoder.onnx'),
    opset_version=14,
    input_names=["latent"],
    output_names=["image"],
    do_constant_folding=True,
    export_params=True,
)

logger.info("VAE decoder exported successfully")
logger.info("Export complete. Models saved to model_repository/stable_diffusion/1/")
```
From the script, the outputs are `text_encoder.onnx`, `unet.onnx`, and `vae_decoder.onnx`.

After the ONNX model is exported, convert the ONNX model to the TensorRT engine serialized file. ([Details](https://github.com/NVIDIA/TensorRT/blob/release/9.2/samples/trtexec/README.md?plain=1#L22) about trtexec cli)

```bash
# Convert text encoder
trtexec --onnx=model_repository/stable_diffusion/1/text_encoder.onnx \
        --saveEngine=model_repository/stable_diffusion/1/text_encoder.engine \
        --fp16 \
        --workspace=4096

# Convert UNet
trtexec --onnx=model_repository/stable_diffusion/1/unet.onnx \
        --saveEngine=model_repository/stable_diffusion/1/unet.engine \
        --fp16 \
        --workspace=4096

# Convert VAE decoder
trtexec --onnx=model_repository/stable_diffusion/1/vae_decoder.onnx \
        --saveEngine=model_repository/stable_diffusion/1/vae_decoder.engine \
        --fp16 \
        --workspace=4096
```

## Part 2: Serving
It is recommended to use the `nvcr.io/nvidia/tritonserver:23.12-py3` image (or later versions) which has the Triton Server python API library installed, and install the ray serve lib by `pip install "ray[serve]"` inside the image.

### Prepare the model repository
Triton Server requires a [model repository](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_repository.md) to store the models, which is a local directory or remote blob store (e.g. AWS S3) containing the model configuration and the model files.
For this example, use the model repository used in Part 1. 

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
