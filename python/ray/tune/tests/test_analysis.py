from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import numpy as np
import pandas as pd
from fix_results import clean_dataframe


TEST_CASES = (
    {
        'kwargs': {
            'dataframe': pd.DataFrame({
                'iterations_since_restore': (
                    *range(1, 4),
                    *range(1, 5),
                ),
            })
        },
        'expected_output': pd.DataFrame({
            'iterations_since_restore': tuple(range(1, 5)),
        }, index=tuple(range(3, 7)))
    },
    {
        'kwargs': {
            'dataframe': pd.DataFrame({
                'iterations_since_restore': (
                    *range(1, 4),
                    *range(1, 3),
                    *range(1, 2),
                ),
            })
        },
        'expected_output': pd.DataFrame({
            'iterations_since_restore': tuple(range(1, 4))
        }, index=tuple(range(0, 3)))
    },
    {
        'kwargs': {
            'dataframe': pd.DataFrame({
                'iterations_since_restore': tuple(range(1, 11)),
            })
        },
        'expected_output': pd.DataFrame({
            'iterations_since_restore': tuple(range(1, 11)),
        }, index=tuple(range(0, 10)))
    },
)


class TestFixResults(unittest.TestCase):
    def test_fix_results(self):
        for test_case in TEST_CASES:
            output = clean_dataframe(**test_case['kwargs'])
            pd.testing.assert_frame_equal(
                output, test_case['expected_output'])
            np.testing.assert_equal(
                output['iterations_since_restore'],
                np.arange(1, output.shape[0] + 1))


if __name__ == '__main__':
    unittest.main()