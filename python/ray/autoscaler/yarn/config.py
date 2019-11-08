from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

logger = logging.getLogger(__name__)

WORKER_SERVICE = "ray-worker-service"


def bootstrap_yarn(config):
    return config
