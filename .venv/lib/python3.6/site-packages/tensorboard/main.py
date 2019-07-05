# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""TensorBoard main module.

This module ties together `tensorboard.program` and
`tensorboard.default_plugins` to provide standard TensorBoard. It's
meant to be tiny and act as little other than a config file. Those
wishing to customize the set of plugins or static assets that
TensorBoard uses can swap out this file with their own.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=g-import-not-at-top
import os

# Disable the TF GCS filesystem cache which interacts pathologically with the
# pattern of reads used by TensorBoard for logdirs. See for details:
#   https://github.com/tensorflow/tensorboard/issues/1225
# This must be set before the first import of tensorflow.
os.environ['GCS_READ_CACHE_DISABLED'] = '1'
# pylint: enable=g-import-not-at-top

import sys

# Import TensorFlow here to fail immediately if it's not present, even though we
# don't actually use it yet, which results in a clearer error.
import tensorflow as tf  # pylint: disable=unused-import

from tensorboard import default
from tensorboard import program


def run_main():
  """Initializes flags and calls main()."""
  program.setup_environment()
  tensorboard = program.TensorBoard(default.get_plugins(),
                                    program.get_default_assets_zip_provider())
  try:
    from absl import app
    # Import this to check that app.run() will accept the flags_parser argument.
    from absl.flags import argparse_flags
    app.run(tensorboard.main, flags_parser=tensorboard.configure)
    raise AssertionError("absl.app.run() shouldn't return")
  except ImportError:
    pass
  tensorboard.configure(sys.argv)
  sys.exit(tensorboard.main())


if __name__ == '__main__':
  run_main()
