The Trainable API
=================

As mentioned in :ref:`Tune User Guide <tune-user-guide>`_, Training can be done
with either a **Class API** (``tune.Trainable``) < or **function-based API** (``track.log``).

Comparably, ``Trainable`` is stateful, supports checkpoint/restore functionality, and provides more control to advanced algorithms.

A naive example for ``Trainable`` is a simple number guesser:

.. code-block:: python

    from ray import tune
    from ray.tune import Trainable

    class Guesser(Trainable):
        def _setup(self, config):
            self.config = config
            self.password = 1024

        def _train(self):
            """Execute one step of 'training'."""
            result_dict = {"diff": abs(self.config['guess'] - self.password)}
            return result_dict

    analysis = tune.run(
        Guesser,
        stop={
            "training_iteration": 1,
        },
        num_samples=10,
        config={
            "guess": tune.randint(1, 10000)
        })

    print('best config: ', analysis.get_best_config(metric="diff", mode="min"))

The program randomly picks 10 number from [1, 10000) and finds which is closer to the password.
As a subclass of ``ray.tune.Trainable``, Tune will convert ``Guesser`` into a Ray actor, which
runs on a separate process on a worker. ``_setup`` function is invoked once for each Actor for custom initialization.

``_train`` execute one logical iteration of training in the tuning process,
which may include several iterations of actual training (see the next example). As a rule of
thumb, the execution time of one train call should be large enough to avoid overheads
(i.e. more than a few seconds), but short enough to report progress periodically
(i.e. at most a few minutes).

We only implemented ``_setup`` and ``_train`` methods for simplification, usually it's also required to implement ``_save``, and ``_restore`` for checkpoint and fault tolerance.


Save and Restore
----------------

When running a hyperparameter search, Tune can automatically and periodically save/checkpoint your model. You can enable this by implementing ``_save``, and ``_restore`` abstract methods, as seen in `this example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__.

For PyTorch model training, this would look something like this `PyTorch example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch_trainable.py>`__:

.. code-block:: python

    class MyTrainableClass(Trainable):
        def _save(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            torch.save(self.model.state_dict(), checkpoint_path)
            return tmp_checkpoint_dir

        def _restore(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            self.model.load_state_dict(torch.load(checkpoint_path))

Checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<iter>``. You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)``.

Tune also generates temporary checkpoints for pausing and switching between trials. For this purpose, it is important not to depend on absolute paths in the implementation of ``save``.

Checking your work
~~~~~~~~~~~~~~~~~~


Use ``validate_save_restore`` to catch ``_save``/``_restore`` errors before execution.

.. code-block:: python

    from ray.tune.utils import validate_save_restore

    # both of these should return
    validate_save_restore(MyTrainableClass)
    validate_save_restore(MyTrainableClass, use_object_store=True)

Advanced: Caching
-----------------

Implement ``Trainable.reset_config`` to avoid large startup overheads.
