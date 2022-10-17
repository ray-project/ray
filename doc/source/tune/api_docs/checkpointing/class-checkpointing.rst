Class API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~

You can also implement checkpoint/restore using the Trainable Class API:

.. code-block:: python

    class MyTrainableClass(Trainable):
        def save_checkpoint(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            torch.save(self.model.state_dict(), checkpoint_path)
            return tmp_checkpoint_dir

        def load_checkpoint(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            self.model.load_state_dict(torch.load(checkpoint_path))

    tuner = tune.Tuner(
        MyTrainableClass,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(checkpoint_frequency=2)
        )
    )
    results = tuner.fit()
