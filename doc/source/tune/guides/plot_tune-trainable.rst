A native example of Trainable
-----------------------------

As mentioned in `Tune User Guide <tune-usage.html#Tune Training API>`_, Training can be done
with either the `Trainable <tune-usage.html#trainable-api>`__ **Class API** or
**function-based API**.

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
