import os

# few
model_name = "--model_name {}"
models = ["resnet18", "resnet50", "resnet101"]

for model in models:
    os.system(f"python benchmark_mnist_jax.py {model_name.format(model)}")

