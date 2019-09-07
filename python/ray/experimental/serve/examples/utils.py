"""
Utility functions for serve examples.
"""

from ray.experimental.serve.utils import pformat_color_json


def pprint_color_json(d):
    """Print a dictionary with as JSON, highlighted and colored."""
    print(pformat_color_json(d))
