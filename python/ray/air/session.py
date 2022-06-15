from typing import Dict, Optional

from ray.air._internal.session import _get_session
from ray.air.checkpoint import Checkpoint


def report(metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
    """Report metrics and optionally save checkpoint.

    Each invocation of this method will automatically increment the underlying
    iteration number. The physical meaning of this "iteration" is defined by
    user (or more specifically the way they call ``report``).
    It does not necessarily map to one epoch.

    This API is supposed to replace the legacy ``tune.report``,
    ``with tune.checkpoint_dir``, ``train.report`` and ``train.save_checkpoint``.
    Please avoid mixing them together.

    There is no requirement on what is the underlying representation of the
    checkpoint.

    All forms are accepted and (will eventually be) handled by AIR in an efficient way.

    Specifically, if you are passing in a directory checkpoint, AIR will move
    the content of the directory to AIR managed directory. By the return of this
    method, one may safely interact with the original directory without
    interfering with AIR checkpointing flow.

    Example:
        .. code-block: python

            from ray.air import session
            from ray.air.checkpoint import Checkpoint
            ######## Using it in the *per worker* train loop (TrainSession) #######
            def train_func():
                model = build_model()
                model.save("my_model", overwrite=True)
                session.report(
                    metrics={"foo": "bar"},
                    checkpoint=Checkpoint.from_directory(temp_dir.name)
                )
                # Air guarantees by this point, you can safely write new stuff to
                # "my_model" directory.
            scaling_config = {"num_workers": 2}
            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func, scaling_config=scaling_config
            )
            result = trainer.fit()
            # If you navigate to result.checkpoint's path, you will find the
            content of ``model.save()`` under it.
            # If you have `SyncConfig` configured, the content should also
            # show up in the corresponding cloud storage path.

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
    """

    _get_session().report(metrics, checkpoint=checkpoint)


def get_checkpoint() -> Optional[Checkpoint]:
    """Access the session's loaded checkpoint to resume from if applicable.

    Returns:
        Checkpoint object if the session is currently being resumed.
        Otherwise, return None.

    Example:
        .. code-block: python

        ######## Using it in the *per worker* train loop (TrainSession) ######
        from ray.air import session
        from ray.air.checkpoint import Checkpoint
        def train_func():
            if session.get_checkpoint():
                with session.get_checkpoint().as_directory() as
                        loaded_checkpoint_dir:
                    import tensorflow as tf
                    model = tf.keras.models.load_model(loaded_checkpoint_dir)
            else:
                model = build_model()

            model.save("my_model", overwrite=True)
            session.report(
                metrics={"iter": 1},
                checkpoint=Checkpoint.from_directory("my_model")
            )

        scaling_config = {"num_workers": 2}
        trainer = TensorflowTrainer(
            train_loop_per_worker=train_func, scaling_config=scaling_config
        )
        result = trainer.fit()

        # trainer2 will pick up from the checkpoint saved by trainer1.
        trainer2 = TensorflowTrainer(
            train_loop_per_worker=train_func,
            scaling_config=scaling_config,
            # this is ultimately what is accessed through
            # ``Session.get_checkpoint()``
            resume_from_checkpoint=result.checkpoint,
        )
        result2 = trainer2.fit()
    """

    return _get_session().loaded_checkpoint


def get_trial_name() -> str:
    """Trial name for the corresponding trial."""
    return _get_session().trial_name


def get_trial_id() -> str:
    """Trial id for the corresponding trial."""
    return _get_session().trial_id


def get_trial_resources() -> Dict[str, float]:
    """Trial resources for the corresponding trial."""
    return _get_session().trial_resources
