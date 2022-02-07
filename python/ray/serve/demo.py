#####################################################
import torch
from ray.serve.demo_model import Summer

adder = Summer(2)
torch.save(adder, "summer.pth")

#####################################################
from ray import serve
from ray.serve.model_wrappers import (
    ModelWrapper,
    TorchCheckpoint,
    TorchModel,
    TorchModelWrapper,
)

serve.start()
# ModelWrapper.deploy(TorchModel, TorchCheckpoint.from_uri("summer.pth"))
# web ui to demo
#####################################################

# Does batching work?
# Now let's add a new adapter type, image to np array

# from ray.serve.http_adapters import image_to_databatch
# ModelWrapper.deploy(
#     TorchModel, TorchCheckpoint.from_uri("summer.pth"), input_schema=image_to_databatch
# )
TorchModelWrapper.deploy("summer.pth", "ray.serve.http_adapters.image_to_databatch")

# also CLI
# serve deploy ray.serve.model_wrappers.TorchModelWrapper --options-json '{"init_args": ["summer.pth", "ray.serve.http_adapters.image_to_databatch"]}'
