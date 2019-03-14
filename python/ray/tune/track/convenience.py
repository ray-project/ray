"""
Miscellaneous helpers for getting some of the arguments to tracking-related
functions automatically, usually involving parameter extraction in a
sensible default way from commonly used libraries.
"""

import sys
from absl import flags

def absl_flags():
    """
    Extracts absl-py flags that the user has specified and outputs their
    key-value mapping.

    By default, extracts only those flags in the current __package__
    and mainfile.

    Useful to put into a trial's param_map.
    """
    # TODO: need same thing for argparse
    flags_dict = flags.FLAGS.flags_by_module_dict()
    # only include parameters from modules the user probably cares about
    def _relevant_module(module_name):
        if __package__ and __package__ in module_name:
            return True
        if module_name == sys.argv[0]:
            return True
        return False
    return {
        flag.name: flag.value for module, flags in flags_dict.items()
        for flag in flags if _relevant_module(module)}
