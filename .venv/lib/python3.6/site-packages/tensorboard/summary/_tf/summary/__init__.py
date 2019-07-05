# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
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
"""TensorFlow component package for providing tf.summary from TensorBoard."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib

import tensorflow as tf

# Re-export all symbols from the original tf.summary.
# pylint: disable=wildcard-import,unused-import,g-import-not-at-top

if getattr(tf, '__version__', '').startswith('2.'):
  from tensorflow.summary import *
else:
  try:
    # Check if we can directly import tf.compat.v2. We may not be able to if we
    # reached this import itself while importing tf.compat.v2. Use importlib
    # to simulate 'import tensorflow.compat.v2' but without binding local names
    # which are hard to clean up.
    #
    # Note that we can't use either of the following internally:
    #
    #   import tensorflow.compat.v2 as other
    #   from tensorflow.compat import v2
    #
    # The former raises AttributeError until python 3.7:
    # <https://bugs.python.org/issue30024>
    # The latter raises ImportError even when it shouldn't, until python 3.5:
    # <https://bugs.python.org/issue17636>
    #
    # In commemoration of this fiasco, I offer the following haiku:
    #
    #   import a module
    #   it works. but add "as other"
    #   AttributeError
    #
    importlib.import_module('tensorflow.compat.v2')
  except ImportError:
    # If that failed, go "under the hood" and attempt to directly import the
    # module that will become tf.compat.v2.summary.
    try:
      from tensorflow._api.v1.compat.v2.summary import *
    except ImportError:
      from tensorflow._api.v2.compat.v2.summary import *
  else:
    from tensorflow.compat.v2.summary import *

from tensorboard.summary.v2 import audio
from tensorboard.summary.v2 import histogram
from tensorboard.summary.v2 import image
from tensorboard.summary.v2 import scalar
from tensorboard.summary.v2 import text

del absolute_import, division, print_function, importlib, tf
