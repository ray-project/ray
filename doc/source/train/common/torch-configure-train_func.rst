First, update your training code to support distributed training.
Begin by wrapping your code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func():
        # Your model training code here.
        ...

Each distributed training worker executes this function.

You can also specify the input argument for `train_func` as a dictionary via the Trainer's `train_loop_config`. For example:

.. testcode:: python
    :skipif: True

    def train_func(config):
        lr = config["lr"]
        num_epochs = config["num_epochs"]

    config = {"lr": 1e-4, "num_epochs": 10}
    trainer = ray.train.torch.TorchTrainer(train_func, train_loop_config=config, ...)

.. warning::

    Avoid passing large data objects through `train_loop_config` to reduce the
    serialization and deserialization overhead. Instead, it's preferred to
    initialize large objects (e.g. datasets, models) directly in `train_func`.

    .. code-block:: diff

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
