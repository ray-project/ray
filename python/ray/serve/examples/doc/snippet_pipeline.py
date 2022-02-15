# flake8: noqa

# __import_start__
from ray.serve import pipeline

# __import_end__

import ray

ray.init(num_cpus=16)


# __simple_pipeline_start__
@pipeline.step
def echo(inp):
    return inp


my_node = echo(pipeline.INPUT)
my_pipeline = my_node.deploy()
assert my_pipeline.call(42) == 42
# __simple_pipeline_end__

del my_pipeline


# __simple_chain_start__
@pipeline.step
def add_one(inp):
    return inp + 1


@pipeline.step
def double(inp):
    return inp ** 2


my_node = double(add_one(pipeline.INPUT))
my_pipeline = my_node.deploy()
assert my_pipeline.call(1) == 4

# __simple_chain_end__

del my_pipeline


# __class_node_start__
@pipeline.step
class Adder:
    def __init__(self, value):
        self.value = value

    def __call__(self, inp):
        return self.value + inp


my_pipeline = Adder(2)(pipeline.INPUT).deploy()
assert my_pipeline.call(2) == 4
# __class_node_end__

del my_pipeline


# __pipeline_configuration_start__
@pipeline.step(execution_mode="actors", num_replicas=2)
def echo(inp):
    return inp


my_pipeline = echo(pipeline.INPUT).deploy()
assert my_pipeline.call(42) == 42
# __pipeline_configuration_end__

del my_pipeline


# __preprocessing_pipeline_example_start__
@pipeline.step(execution_mode="tasks")
def preprocess(img_bytes):
    from torchvision import transforms
    import PIL.Image
    import io

    preprocessor = transforms.Compose(
        [
            transforms.Resize(224),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Lambda(lambda t: t[:3, ...]),  # remove alpha channel
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    return preprocessor(PIL.Image.open(io.BytesIO(img_bytes))).unsqueeze(0)


@pipeline.step(execution_mode="actors", num_replicas=2)
class ClassificationModel:
    def __init__(self, model_name):
        import torchvision.models.resnet

        self.model = getattr(torchvision.models.resnet, model_name)(pretrained=True)

    def __call__(self, inp_tensor):
        import torch

        with torch.no_grad():
            output = self.model(inp_tensor).squeeze()
            sorted_value, sorted_idx = output.sort()
        return {
            "top_5_categories": sorted_idx.numpy().tolist()[-5:],
            "top_5_scores": sorted_value.numpy().tolist()[-5:],
        }


import PIL.Image
import io
import numpy as np

# Generate dummy input
_buffer = io.BytesIO()
PIL.Image.fromarray(np.zeros((720, 720, 3), int), mode="RGB").save(_buffer, "png")
dummy_png_bytes = _buffer.getvalue()

sequential_pipeline = ClassificationModel("resnet18")(
    preprocess(pipeline.INPUT)
).deploy()
result = sequential_pipeline.call(dummy_png_bytes)
assert result["top_5_categories"] == [898, 412, 600, 731, 463]
# __preprocessing_pipeline_example_end__

# __cleanup_example_start__
del sequential_pipeline
# __cleanup_example_end__


# __ensemble_pipeline_example_start__
@pipeline.step(execution_mode="tasks")
def combine_output(*classifier_outputs):
    # Here will we will just concatenate the result from multiple models
    # You can easily extend this to other ensemble techniques like voting
    # or weighted average.
    return sum([out["top_5_categories"] for out in classifier_outputs], [])


preprocess_node = preprocess(pipeline.INPUT)
model_nodes = [
    ClassificationModel(model)(preprocess_node) for model in ["resnet18", "resnet34"]
]
ensemble_pipeline = combine_output(*model_nodes).deploy()
result = ensemble_pipeline.call(dummy_png_bytes)
assert result == [898, 412, 600, 731, 463, 899, 618, 733, 463, 600]

# __ensemble_pipeline_example_end__

del ensemble_pipeline


# __biz_logic_start__
@pipeline.step(execution_mode="tasks")
def dynamic_weighting_combine(*classifier_outputs):
    # Pseudo-code:
    # Example of bringing in custom business logic and arbitrary Python code.
    # You can issue database queries, log metrics, and run complex computation.
    my_weights = my_db.get("dynamic_weights")
    weighted_output = average(classifier_outputs, my_weights)
    my_logger.log(weighted_output)
    my_api_response = my_response_model.reshape(
        [out.astype("int") for out in weighted_output]
    )
    return my_api_response


# __biz_logic_end__
