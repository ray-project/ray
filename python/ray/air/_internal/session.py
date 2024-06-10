import logging

logger = logging.getLogger(__name__)


# TODO(justinvyu): Move this to train
def _get_session(warn: bool = False):
    from ray.train._internal.session import _session as train_session

    return train_session
