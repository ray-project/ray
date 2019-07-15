Tune Checkpointing
==================

When running a hyperparameter search, Tune can automatically and periodically checkpoint your model. Checkpointing is used for

 * saving a model at the end of training
 * modifying a model in the middle of training
 * fault-tolerance in experiments with pre-emptible machines.

To enable checkpointing, you must implement a `Trainable class <tune-usage.html#training-api>`__ (Trainable functions are not checkpointable, since they never return control back to their caller). The easiest way to do this is to subclass the pre-defined ``Trainable`` class and implement ``_save``, and ``_restore`` abstract methods, as seen in `this example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__.

Note that implementing this interface is required in certain Trial Schedulers such as HyperBand and PBT.

For TensorFlow model training, this would look something like this `tensorflow example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_ray_hyperband.py>`__:

.. code-block:: python

    class MyClass(Trainable):
        def _setup(self, config):
            self.saver = tf.train.Saver()
            self.sess = ...

        def _train(self):
            return {"mean_accuracy: self.sess.run(...)}

        def _save(self, checkpoint_dir):
            return self.saver.save(self.sess, os.path.join(checkpoint_dir, save))

        def _restore(self, checkpoint_prefix):
            self.saver.restore(self.sess, checkpoint_prefix)


Trainable (Trial) Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Checkpointing assumes that the model state will be saved to disk on whichever node the Trainable is running on. Tune will automatically sync that folder
with driver.

A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True`` (or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `_train`. This can be especially helpful in spot instances:

.. code-block:: python

    def _train(self):
        # training code
        # training code

        result = {"mean_accuracy": accuracy}

        if detect_instance_preemption():
            result.update(should_checkpoint=True)

        return result


Additionally, periodic checkpointing can be used to provide fault-tolerance for experiments. This can be enabled by setting ``checkpoint_freq=N`` and ``max_failures=M`` to checkpoint trials every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python
   :emphasize-lines: 4,5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        max_failures=5,
    )

The checkpoint_freq may not coincide with the exact end of an experiment. If you want a checkpoint to be created at the end
of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. code-block:: python
   :emphasize-lines: 5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        checkpoint_at_end=True,
        max_failures=5,
    )

Fault Tolerance
~~~~~~~~~~~~~~~

Tune will automatically restart trials in case of trial failures (if ``max_failures`` is set).

For example, if a node is lost while a trial (specifically, the corresponding Trainable actor of the trial) is still executing on that node and a checkpoint of the trial exists, Tune will wait until available resources are available to begin executing the trial again. If the trial/actor is placed on a different node, Tune will automatically push the previous checkpoint file to that node and restore the remote trial actor state, allowing the trial to resume from the latest checkpoint even after failure.


Recovering From Failures
~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically persists the progress of your entire experiment (a `tune.run` session), so if an experiment crashes or is otherwise cancelled, it can be resumed by passing one of True, False, "LOCAL", "REMOTE", or "PROMPT" to ``tune.run(resume=...)``. Note that this only works if trial checkpoints are detected, whether it be by manual or periodic checkpointing.

The default setting of ``resume=False`` creates a new experiment. ``resume="LOCAL"`` and ``resume=True`` restore the experiment from ``local_dir/[experiment_name]``. ``resume="REMOTE"`` syncs the upload dir down to the local dir and then restores the experiment from ``local_dir/experiment_name``. ``resume="PROMPT"`` will cause Tune to prompt you for whether you want to resume. You can always force a new experiment to be created by changing the experiment name.

Note that trials will be restored to their last checkpoint. If trial checkpointing is not enabled, unfinished trials will be restarted from scratch.

E.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        local_dir="~/path/to/results",
        resume=True
    )


Upon a second run, this will restore the entire experiment state from ``~/path/to/results/my_experiment_name``. Importantly, any changes to the experiment specification upon resume will be ignored.

This feature is still experimental, so any provided Trial Scheduler or Search Algorithm will not be preserved. Only ``FIFOScheduler`` and ``BasicVariantGenerator`` will be supported.
