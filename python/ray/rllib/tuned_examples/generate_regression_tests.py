#!/usr/bin/env python
# This script generates all the regression tests for RLlib.

import glob
import re
import os
import os.path as osp

CONFIG_DIR = osp.join(osp.dirname(osp.abspath(__file__)), "regression_tests")

TEMPLATE = """
class Test{name}(Regression):
    _file = "{filename}"

    def setup_cache(self):
        return _evaulate_config(self._file)

"""

if __name__ == '__main__':
    os.chdir(CONFIG_DIR)

    with open("regression_test.py", "a") as f:
        for filename in sorted(glob.glob("*.yaml")):
            splits = re.findall(r"\w+", osp.splitext(filename)[0])
            test_name = "".join([s.capitalize() for s in splits])
            f.write(TEMPLATE.format(name=test_name, filename=filename))
