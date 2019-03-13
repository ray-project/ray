from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class TuneError(Exception):
    """General error class raised by ray.tune."""
    pass


class AbortTrialExecution(TuneError):
    """Error that indicates a trial should not be retried."""
    pass
