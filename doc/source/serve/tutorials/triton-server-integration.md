---
orphan: true
---
# Serving models with Triton Server in Ray Serve
This guide shows how to build an application with stable diffusion model using [NVIDIA Triton Server](https://github.com/triton-inference-server/server) in Ray Serve.

## Part 0: Create a directory structure and dockerfile using nvidia's base image
Setup following directory structure in your app:
```
.
├── Dockerfile
├── serve_app.py
└── models/
    ├── text_encoder/
    ├── unet/
    └── vae_decoder/
```
Once setup, update the dockerfile with following base image and commands:

```
# Base image with CUDA + Triton
FROM nvcr.io/nvidia/tritonserver:24.03-py3

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    git \
    curl \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --upgrade pip

# Install Ray, FastAPI, Triton client, etc.
RUN pip install \
    ray[serve]==2.10.0 \
    fastapi \
    uvicorn[standard] \
    transformers \
    torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121 \
    onnxruntime-gpu \
    tritonclient[http] \
    diffusers \
    accelerate \
    pillow

# Copy your app into the container
COPY serve_app.py .

# Expose Ray Serve and Triton default ports
EXPOSE 8000 8001 8002

# Set environment variables for Triton
ENV MODEL_REPOSITORY=/models
ENV NVIDIA_VISIBLE_DEVICES=all

# Run Triton + Ray Serve in the same container
CMD tritonserver --model-repository=/models & \
    sleep 10 && \
    python serve_app.py
```
## Part 1: Compile the model to onnx/tensorrt format
Using pytorch apis, convert the submodels (in case of stable diffusion) to onnx artifacts and store them with the repository structure mentioned above.

```python
import torch
from diffusers import StableDiffusionPipeline
import os
import logging
from pathlib import Path
import sys

# Load a specific model version that's known to work well with ONNX conversion
model_id = "runwayml/stable-diffusion-v1-5"  # This is often the most compatible
model_path = Path("model")

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
    str(model_path / 'text_encoder/text_encoder.onnx'),
    opset_version=14,
    input_names=["input_ids"],
    output_names=["last_hidden_state"],
    do_constant_folding=True,
    export_params=True,
)

# Export UNet
torch.onnx.export(
    unet,
    (dummy_sample, timestep, encoder_hidden_states),
    str(model_path / 'unet/unet.onnx'),
    opset_version=14,
    input_names=["sample", "timestep", "encoder_hidden_states"],
    output_names=["out_sample"],
    do_constant_folding=True,
    export_params=True,
)

# Export VAE decoder
dummy_latent = torch.randn(batch_size, 4, 64, 64, device=device)

torch.onnx.export(
    pipe.vae.decoder,
    dummy_latent,
    str(model_path / 'vae_decoder/vae_decoder.onnx'),
    opset_version=14,
    input_names=["latent"],
    output_names=["image"],
    do_constant_folding=True,
    export_params=True,
)
```

## Part 2: Build the python backend & start the server
The python backend wraps the triton server with serve app. Following code uses the prebuilt `tritonserver` available within the triton docker image for the purpose.
```

The model repository contains three models: `stable_diffusion`, `text_encoder` and `vae`. Each model has an optional `config.pbtxt` file and a model file. For the tutorial, this file is omitted for brevity. The `config.pbtxt` file contains the model configuration, which is used to describe the model type and input/output formats.(you can learn more about model config file [here](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_configuration.md)). To get config files for our example, you can download them from [here](https://github.com/triton-inference-server/tutorials/tree/main/Conceptual_Guide/Part_6-building_complex_pipelines/model_repository). We use `1` as the version of each model. The model files are saved in the version directory.

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
