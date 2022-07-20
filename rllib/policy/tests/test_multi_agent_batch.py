import unittest

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.test_utils import check_same_batch


class TestMultiAgentBatch(unittest.TestCase):
    def test_timeslices_non_overlapping_experiences(self):
        """Tests if timeslices works as expected on a MultiAgentBatch
        consisting of two non-overlapping SampleBatches.
        """

        def _generate_data(agent_idx):
            batch = SampleBatch(
                {
                    SampleBatch.T: [0, 1],
                    SampleBatch.EPS_ID: 2 * [agent_idx],
                    SampleBatch.AGENT_INDEX: 2 * [agent_idx],
                    SampleBatch.SEQ_LENS: [2],
                }
            )
            return batch

        policy_batches = {str(idx): _generate_data(idx) for idx in (range(2))}
        ma_batch = MultiAgentBatch(policy_batches, 4)
        sliced_ma_batches = ma_batch.timeslices(1)

        [
            check_same_batch(i, j)
            for i, j in zip(
                sliced_ma_batches,
                [
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [0],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [0],
                                    SampleBatch.EPS_ID: [1],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [1],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                ],
            )
        ]

    def test_timeslices_partially_overlapping_experiences(self):
        """Tests if timeslices works as expected on a MultiAgentBatch
        consisting of two partially overlapping SampleBatches.
        """

        def _generate_data(agent_idx, t_start):
            batch = SampleBatch(
                {
                    SampleBatch.T: [t_start, t_start + 1],
                    SampleBatch.EPS_ID: [0, 0],
                    SampleBatch.AGENT_INDEX: 2 * [agent_idx],
                    SampleBatch.SEQ_LENS: [2],
                }
            )
            return batch

        policy_batches = {str(idx): _generate_data(idx, idx) for idx in (range(2))}
        ma_batch = MultiAgentBatch(policy_batches, 4)
        sliced_ma_batches = ma_batch.timeslices(1)

        [
            check_same_batch(i, j)
            for i, j in zip(
                sliced_ma_batches,
                [
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [0],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [2],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            )
                        },
                        1,
                    ),
                ],
            )
        ]

    def test_timeslices_fully_overlapping_experiences(self):
        """Tests if timeslices works as expected on a MultiAgentBatch
        consisting of two fully overlapping SampleBatches.
        """

        def _generate_data(agent_idx):
            batch = SampleBatch(
                {
                    SampleBatch.T: [0, 1],
                    SampleBatch.EPS_ID: [0, 0],
                    SampleBatch.AGENT_INDEX: 2 * [agent_idx],
                    SampleBatch.SEQ_LENS: [2],
                }
            )
            return batch

        policy_batches = {str(idx): _generate_data(idx) for idx in (range(2))}
        ma_batch = MultiAgentBatch(policy_batches, 4)
        sliced_ma_batches = ma_batch.timeslices(1)

        [
            check_same_batch(i, j)
            for i, j in zip(
                sliced_ma_batches,
                [
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [0],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [0],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                        },
                        1,
                    ),
                    MultiAgentBatch(
                        {
                            "0": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [0],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                            "1": SampleBatch(
                                {
                                    SampleBatch.T: [1],
                                    SampleBatch.EPS_ID: [0],
                                    SampleBatch.AGENT_INDEX: [1],
                                    SampleBatch.SEQ_LENS: [1],
                                }
                            ),
                        },
                        1,
                    ),
                ],
            )
        ]


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
