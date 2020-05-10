import logging

from ray.tune import session

logger = logging.getLogger(__name__)

_session = None
warned = False


def _deprecation_warning(soft=True):
    msg = "tune.track is now deprecated. Use `tune.report` instead."
    global warned
    if soft:
        msg += " This warning will throw an error in a future version of Ray."
        if not warned:
            logger.warning(msg)
            warned = True
    else:
        raise DeprecationWarning(msg)


def get_session():
    _deprecation_warning(soft=False)


def init(ignore_reinit_error=True, **session_kwargs):
    _deprecation_warning(soft=False)


def shutdown():
    _deprecation_warning(soft=False)


def log(**kwargs):
    _deprecation_warning(soft=True)
    session.report(**kwargs)


def trial_dir():
    _deprecation_warning(soft=True)
    return _session.logdir


def trial_name():
    _deprecation_warning(soft=True)
    return session.trial_name


def trial_id():
    _deprecation_warning(soft=True)
    return session.trial_id
