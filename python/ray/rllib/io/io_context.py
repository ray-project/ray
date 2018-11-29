from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class IOContext(object):
    """Attributes to pass to input / output class constructors.

    RLlib auto-sets these attributes when constructing input / output classes.

    Attributes:
        log_dir (str): Default logging directory.
        config (dict): Configuration of the agent.
        worker_index (int): When there are multiple workers created, this
            uniquely identifies the current worker.
        evaluator (PolicyEvaluator): policy evaluator object reference.
    """

    def __init__(self, log_dir, config, worker_index, evaluator):
        self.log_dir = log_dir
        self.config = config
        self.worker_index = worker_index
        self.evaluator = evaluator
