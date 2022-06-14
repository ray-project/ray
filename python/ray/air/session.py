import abc
import logging
from typing import Dict, Optional

from ray.air.checkpoint import Checkpoint

logger = logging.getLogger(__name__)


class Session(abc.ABC):
    """The canonical session interface that both Tune and Train session implements.

    User can interact with this interface to get session information,
    as well as reporting metrics and saving checkpoint.
    """

    @abc.abstractmethod
    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
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

        All forms are accepted and (will be) handled by AIR in an efficient way.

        Specifically, if you are passing in a directory checkpoint, AIR will move
        the content of the directory to AIR managed directory. By the return of this
        method, one may safely write new content to the original directory without
        interfering with AIR checkpointing flow.

        Example:
            .. code-block: python

                from ray.air.session import get_session
                from ray.air.checkpoint import Checkpoint
                ######## Using it in the *per worker* train loop (TrainSession) #######
                def train_func():
                    session = get_session()
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

        raise NotImplementedError

    @property
    @abc.abstractmethod
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        """Access the session's loaded checkpoint to resume from if applicable.

        Returns:
            Checkpoint object if the session is currently being resumed.
            Otherwise, return None.

        Example:
            .. code-block: python

            ######## Using it in the *per worker* train loop (TrainSession) ######
            def train_func():
                session = get_session()
                if session.loaded_checkpoint:
                    with session.loaded_checkpoint.as_directory() as
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
                # ``Session.loaded_checkpoint``
                resume_from_checkpoint=result.checkpoint,
            )
            result2 = trainer2.fit()
        """

        raise NotImplementedError

    @property
    def trial_name(self) -> str:
        """Trial name for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_id(self) -> str:
        """Trial id for the corresponding trial."""
        raise NotImplementedError

    @property
    def trial_resources(self) -> Dict[str, float]:
        """Trial resources for the corresponding trial."""
        raise NotImplementedError


def get_session() -> Optional[Session]:
    from ray.train._internal.session import _session_v2 as train_session
    from ray.tune.session import _session_v2 as tune_session

    if train_session and tune_session:
        logger.warning(
            "Expected to be either in tune session or train session but not both."
        )
        return None
    if not (train_session or tune_session):
        logger.warning("In neither tune session nor train session!")
        return None
    return train_session or tune_session
