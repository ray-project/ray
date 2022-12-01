import unittest

import gym

from ray.rllib.connectors.connector import Connector, ConnectorPipeline
from ray.rllib.connectors.connector import ConnectorContext
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector
from ray.rllib.connectors.agent.mean_std_filter import (
    MeanStdObservationFilterAgentConnector,
)
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector


class TestConnectorPipeline(unittest.TestCase):
    class Tom(Connector):
        def to_state():
            return "tom"

    class Bob(Connector):
        def to_state():
            return "bob"

    class Mary(Connector):
        def to_state():
            return "mary"

    class MockConnectorPipeline(ConnectorPipeline):
        def __init__(self, ctx, connectors):
            # Real connector pipelines should keep a list of
            # Connectors.
            # Use strings here for simple unit tests.
            self.connectors = connectors

    def test_sanity_check(self):
        ctx = {}

        m = self.MockConnectorPipeline(ctx, [self.Tom(ctx), self.Bob(ctx)])
        m.insert_before("Bob", self.Mary(ctx))
        self.assertEqual(len(m.connectors), 3)
        self.assertEqual(m.connectors[1].__class__.__name__, "Mary")

        m = self.MockConnectorPipeline(ctx, [self.Tom(ctx), self.Bob(ctx)])
        m.insert_after("Tom", self.Mary(ctx))
        self.assertEqual(len(m.connectors), 3)
        self.assertEqual(m.connectors[1].__class__.__name__, "Mary")

        m = self.MockConnectorPipeline(ctx, [self.Tom(ctx), self.Bob(ctx)])
        m.prepend(self.Mary(ctx))
        self.assertEqual(len(m.connectors), 3)
        self.assertEqual(m.connectors[0].__class__.__name__, "Mary")

        m = self.MockConnectorPipeline(ctx, [self.Tom(ctx), self.Bob(ctx)])
        m.append(self.Mary(ctx))
        self.assertEqual(len(m.connectors), 3)
        self.assertEqual(m.connectors[2].__class__.__name__, "Mary")

        m.remove("Bob")
        self.assertEqual(len(m.connectors), 2)
        self.assertEqual(m.connectors[0].__class__.__name__, "Tom")
        self.assertEqual(m.connectors[1].__class__.__name__, "Mary")

        m.remove("Bob")
        # Bob does not exist anymore, still 2.
        self.assertEqual(len(m.connectors), 2)
        self.assertEqual(m.connectors[0].__class__.__name__, "Tom")
        self.assertEqual(m.connectors[1].__class__.__name__, "Mary")

        self.assertEqual(m["Tom"], [m.connectors[0]])
        self.assertEqual(m[0], [m.connectors[0]])
        self.assertEqual(m[m.connectors[1].__class__], [m.connectors[1]])

    def test_pipeline_indexing(self):
        """Tests if ConnectorPipeline.__getitem__() works as intended."""
        context = ConnectorContext({}, observation_space=gym.spaces.Box(-1, 1, (1,)))
        some_connector = MeanStdObservationFilterAgentConnector(context)
        some_other_connector = ObsPreprocessorConnector(context)
        # Create a dummy pipeline just for indexing purposes
        pipeline = ConnectorPipeline(context, [some_connector, some_other_connector])

        for key, expected_value in [
            (MeanStdObservationFilterAgentConnector, [some_connector]),
            ("MeanStdObservationFilterAgentConnector", [some_connector]),
            (SyncedFilterAgentConnector, [some_connector]),
            ("SyncedFilterAgentConnector", []),
            (ClipRewardAgentConnector, []),
            ("can i get something?", []),
            (0, [some_connector]),
            (1, [some_other_connector]),
        ]:
<<<<<<< HEAD
=======
            print(key)
            print(expected_value)
>>>>>>> c3c62543a (initial)
            self.assertEqual(pipeline[key], expected_value)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
