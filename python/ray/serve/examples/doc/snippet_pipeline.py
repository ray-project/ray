# __import_start__
import PIL
from ray.serve import pipeline
# __import_end__

import ray
ray.init(num_cpus=16)


# __simple_pipeline_start__
@pipeline.step
def echo(inp):
    return inp


my_pipeline = echo(pipeline.INPUT).deploy()
assert my_pipeline.call(42) == 42
# __simple_pipeline_end__


# __pipeline_configuration_start__
@pipeline.step(execution_mode="actors", num_replicas=2)
def echo(inp):
    return inp


my_pipeline = echo(pipeline.INPUT).deploy()
assert my_pipeline.call(42) == 42
# __pipeline_configuration_end__


# __preprocessing_pipeline_examples__
@pipeline.step(execution_mode="tasks")
def preprocess(img_bytes):
    from torchvision import transforms
    import PIL.Image
    import io

    preprocessor = transforms.Compose([
        transforms.Resize(224),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Lambda(lambda t: t[:3, ...]),  # remove alpha channel
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    return preprocessor(PIL.Image.open(io.BytesIO(img_bytes))).unsqueeze(0)


@pipeline.step(execution_mode="actors", num_replicas=2)
class ClassificationModel:
    def __init__(self, model_name):
        import torchvision.models.resnet
        self.model = getattr(torchvision.models.resnet,
                             model_name)(pretrained=True)

    def __call__(self, inp_tensor):
        import torch
        with torch.no_grad():
            output = self.model(inp_tensor).squeeze()
        return {
            "top_5_categories": output.argsort().cpu().numpy().tolist()[-5:]
        }


import PIL.Image
import io
import numpy as np

_buffer = io.BytesIO()
PIL.Image.fromarray(
    np.zeros((720, 720, 3), int), mode="RGB").save(_buffer, "png")
dummy_png_bytes = _buffer.getvalue()

sequential_pipeline = (ClassificationModel("resnet18")(preprocess(
    pipeline.INPUT)).deploy())
assert sequential_pipeline.call(dummy_png_bytes) == {
    'top_5_categories': [898, 412, 600, 731, 463]
}
