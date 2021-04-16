import os

codes = ["mnist_jax_example.py", "sst_jax_example.py", "mnist_flax_example.py", "wiki_flax_example.py"]

# 2,3,4,5,6, 7,8
num_workers = "--num-workers {}"
num_workers_candidate = range(2,9)

# few
model_names = "--model-name {}"
models_candidate = ["resnet18", "resnet50", "resnet101"]

# strategy
strategy = "--trainer {}"
strategy_candidate = ["ar", "ps"]

woker2cuda = {2: "0,2"}

for trainer in strategy_candidate:
    for model in models_candidate:
        os.system(f"python mnist_jax_example.py --num-epochs 5 --num-workers 2 {model_names.format(model)} {strategy.format(trainer)}")



