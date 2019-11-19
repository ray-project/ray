# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import unittest

from ray.tune.checkpoint_policy import BasicCheckpointPolicy, logger

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


class BasicCheckpointPolicyTest(unittest.TestCase):

    def testShouldCheckpoint(self):
        pass

    def testCheckpointScore(self):
        pass

    def testCheckpointScoreUnavailableAttribute(self):
        """
        Tests that an error is logged when the associated result of the
        checkpoint has no checkpoint score attribute.
        """
        keep_checkpoints_num = 1
        checkpoint_manager = BasicCheckpointPolicy(keep_checkpoints_num,
                                                   scoring_attribute="i")

        no_attr_result = {}
        with patch.object(logger, "error") as log_error_mock:
            checkpoint_manager.checkpoint_score(no_attr_result)
            log_error_mock.assert_called_once()
