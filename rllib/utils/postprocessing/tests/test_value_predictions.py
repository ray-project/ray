import unittest

from ray.rllib.utils.postprocessing.value_predictions import extract_bootstrapped_values
from ray.rllib.utils.test_utils import check


class TestPostprocessing(unittest.TestCase):
    def test_extract_bootstrapped_values(self):
        """Tests, whether the extract_bootstrapped_values utility works properly."""

        # Fake vf_preds sequence.
        # Spaces = denote (elongated-by-one-artificial-ts) episode boundaries.
        # digits = timesteps within the actual episode.
        # [lower case letters] = bootstrap values at episode truncations.
        # '-' = bootstrap values at episode terminals (these values are simply zero).
        sequence = "012345678a 01234A 0- 0123456b 01c 012- 012345e 012-"
        sequence = sequence.replace(" ", "")
        sequence = list(sequence)
        # The actual, non-elongated, episode lengths.
        episode_lengths = [9, 5, 1, 7, 2, 3, 6, 3]
        T = 4
        result = extract_bootstrapped_values(
            vf_preds=sequence,
            episode_lengths=episode_lengths,
            T=T,
        )
        check(result, [4, 8, 3, 1, 5, "c", 1, 5, "-"])

        # Another example.
        sequence = "0123a 012345b 01234567- 012- 012- 012- 012345- 0123456c"
        sequence = sequence.replace(" ", "")
        sequence = list(sequence)
        episode_lengths = [4, 6, 8, 3, 3, 3, 6, 7]
        T = 5
        result = extract_bootstrapped_values(
            vf_preds=sequence,
            episode_lengths=episode_lengths,
            T=T,
        )
        check(result, [1, "b", 5, 2, 1, 3, 2, "c"])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
