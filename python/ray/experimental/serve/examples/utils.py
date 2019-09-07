"""
Utility functions for serve examples.
"""

from ray.experimental.serve.utils import pformat_color_json


def pprint_color_json(d):
    print(pformat_color_json(d))
