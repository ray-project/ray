# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
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
"""A SessionRunHook extends `session.run()` calls for the `MonitoredSession`.

SessionRunHooks are useful to track training, report progress, request early
stopping and more. SessionRunHooks use the observer pattern and notify at the
following points:
 - when a session starts being used
 - before a call to the `session.run()`
 - after a call to the `session.run()`
 - when the session closed

A SessionRunHook encapsulates a piece of reusable/composable computation that
can piggyback a call to `MonitoredSession.run()`. A hook can add any
ops-or-tensor/feeds to the run call, and when the run call finishes with success
gets the outputs it requested. Hooks are allowed to add ops to the graph in
`hook.begin()`. The graph is finalized after the `begin()` method is called.

There are a few pre-defined hooks:
 - StopAtStepHook: Request stop based on global_step
 - CheckpointSaverHook: saves checkpoint
 - LoggingTensorHook: outputs one or more tensor values to log
 - NanTensorHook: Request stop if given `Tensor` contains Nans.
 - SummarySaverHook: saves summaries to a summary writer

For more specific needs, you can create custom hooks:
  class ExampleHook(SessionRunHook):
    def begin(self):
      # You can add ops to the graph here.
      print('Starting the session.')
      self.your_tensor = ...

    def after_create_session(self, session, coord):
      # When this is called, the graph is finalized and
      # ops can no longer be added to the graph.
      print('Session created.')

    def before_run(self, run_context):
      print('Before calling session.run().')
      return SessionRunArgs(self.your_tensor)

    def after_run(self, run_context, run_values):
      print('Done running one step. The value of my tensor: %s',
            run_values.results)
      if you-need-to-stop-loop:
        run_context.request_stop()

    def end(self, session):
      print('Done with the session.')

To understand how hooks interact with calls to `MonitoredSession.run()`,
look at following code:
  with MonitoredTrainingSession(hooks=your_hooks, ...) as sess:
    while not sess.should_stop():
      sess.run(your_fetches)

Above user code leads to following execution:
  call hooks.begin()
  sess = tf.Session()
  call hooks.after_create_session()
  while not stop is requested:
    call hooks.before_run()
    try:
      results = sess.run(merged_fetches, feed_dict=merged_feeds)
    except (errors.OutOfRangeError, StopIteration):
      break
    call hooks.after_run()
  call hooks.end()
  sess.close()

Note that if sess.run() raises OutOfRangeError or StopIteration then
hooks.after_run() will not be called but hooks.end() will still be called.
If sess.run() raises any other exception then neither hooks.after_run() nor
hooks.end() will be called.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from tensorflow.python.training.session_run_hook import SessionRunArgs
from tensorflow.python.training.session_run_hook import SessionRunContext
from tensorflow.python.training.session_run_hook import SessionRunHook
from tensorflow.python.training.session_run_hook import SessionRunValues
from tensorflow.python.util.tf_export import estimator_export

estimator_export("estimator.SessionRunHook")(SessionRunHook)
estimator_export("estimator.SessionRunArgs")(SessionRunArgs)
estimator_export("estimator.SessionRunContext")(SessionRunContext)
estimator_export("estimator.SessionRunValues")(SessionRunValues)
