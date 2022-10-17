Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance.
You can save and load checkpoint in Ray Tune in the following manner:

.. code-block:: python

        import time
        from ray import tune
        from ray.air import session
        from ray.air.checkpoint import Checkpoint

        def train_func(config):
            step = 0
            loaded_checkpoint = session.get_checkpoint()
            if loaded_checkpoint:
                last_step = loaded_checkpoint.to_dict()["step"]
                step = last_step + 1

            for iter in range(step, 100):
                time.sleep(1)

                checkpoint = Checkpoint.from_dict({"step": step})
                session.report({"message": "Hello world Ray Tune!"}, checkpoint=checkpoint)

        tuner = tune.Tuner(train_func)
        results = tuner.fit()

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``<local_dir>/<exp_name>/trial_name/checkpoint_<step>``.

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.
