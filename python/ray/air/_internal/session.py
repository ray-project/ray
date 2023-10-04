import logging


logger = logging.getLogger(__name__)


def _get_session(warn: bool = False):
    from ray.train._internal.session import _session as train_session
    from ray.tune.trainable.session import _session as tune_session

    if train_session and tune_session:
        if warn:
            logger.warning(
                "Expected to be either in tune session or train session but not both."
            )
        return None
    if not (train_session or tune_session):
        if warn:
            logger.warning("In neither tune session nor train session!")
        return None
    return train_session or tune_session
