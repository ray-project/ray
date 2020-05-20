import logging

from ray.tune import session

logger = logging.getLogger(__name__)

_session = None
warned = False


def _deprecation_warning(call=None, alternative_call=None, soft=True):
    msg = "tune.track is now deprecated."
    if call and alternative_call:
        msg = "tune.track.{} is now deprecated.".format(call)
        msg += " Use `tune.{}` instead.".format(alternative_call)
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
    _deprecation_warning(call="log", alternative_call="report", soft=True)
    session.report(**kwargs)


def trial_dir():
    _deprecation_warning(
        call="trial_dir", alternative_call="get_trial_dir", soft=True)
    return session.get_trial_dir()


def trial_name():
    _deprecation_warning(
        call="trial_name", alternative_call="get_trial_name", soft=True)
    return session.get_trial_name()


def trial_id():
    _deprecation_warning(
        call="trial_id", alternative_call="get_trial_id", soft=True)
    return session.get_trial_id()
