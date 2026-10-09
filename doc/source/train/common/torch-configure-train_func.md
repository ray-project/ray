First, update your training code to support distributed training. Wrap your code in a {ref}`training function <train-overview-training-function>`:

```{testcode}
:skipif: True

def train_func():
    # Your model training code here.
    ...
```

Each distributed training worker executes this function.

You can also pass `train_func` a dictionary as its input argument through the `train_loop_config` parameter of `TorchTrainer`. The following example passes a learning rate and a number of epochs:

```{testcode} python
:skipif: True

def train_func(config):
    lr = config["lr"]
    num_epochs = config["num_epochs"]

config = {"lr": 1e-4, "num_epochs": 10}
trainer = ray.train.torch.TorchTrainer(train_func, train_loop_config=config, ...)
```

:::{warning}
To reduce serialization and deserialization overhead, avoid passing large data objects through `train_loop_config`. Instead, initialize large objects, such as datasets and models, directly in `train_func`.

```diff
 def load_dataset():
     # Return a large in-memory dataset
     ...

 def load_model():
     # Return a large in-memory model instance
     ...

-config = {"data": load_dataset(), "model": load_model()}

 def train_func(config):
-    data = config["data"]
-    model = config["model"]

+    data = load_dataset()
+    model = load_model()
     ...

 trainer = ray.train.torch.TorchTrainer(train_func, train_loop_config=config, ...)
```
:::
