import unittest

from ray.rllib.connectors.connector import Connector, ConnectorPipeline


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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
