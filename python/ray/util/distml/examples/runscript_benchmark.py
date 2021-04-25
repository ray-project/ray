import os

# few
model_name = "--model-name {}"
models = ["resnet18", "resnet50", "resnet101"]

for model in models:
    os.system(f"python benchmark_mnist_jax.py {model_name.format(model)}")
    
os.system(f"python benchmark_wiki_flax.py")

