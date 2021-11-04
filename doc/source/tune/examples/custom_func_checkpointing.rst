If want to use checkpointing with a custom training function (not a Ray integration like PyTorch or Tensorflow), you must expose a ``checkpoint_dir`` argument in the function signature, and call ``tune.checkpoint_dir``:

.. code-block:: python

    import os
    import time
    from ray import tune

    def train_func(config, checkpoint_dir=None):
        start = 0
        if checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
                state = json.loads(f.read())
                start = state["step"] + 1

        for step in range(start, 100):
            time.sleep(1)

            # Obtain a checkpoint directory
            with tune.checkpoint_dir(step=step) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": step}))

            tune.report(hello="world", ray="tune")

    tune.run(train_func)

You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)`` By doing this, you can change whatever experiments' configuration such as the experiment's name:
