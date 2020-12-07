#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""A streaming word-counting workflow.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount_with_metrics import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from runner import RayWorkerEnvironment


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    from apache_beam.portability.api import beam_runner_api_pb2
    from apache_beam.runners.portability.fn_api_runner import FnApiRunner
    from apache_beam.portability import python_urns
    import sys

    #  env = beam_runner_api_pb2.Environment(urn="ray_worker")
    #  print(env)
    #  runner = FnApiRunner(default_environment=env)
    cmd = 'python -m apache_beam.runners.worker.sdk_worker_main'

    runner = FnApiRunner(default_environment=RayWorkerEnvironment(cmd))

    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    import ray
    ray.init()
    with beam.Pipeline(options=pipeline_options, runner=runner) as p:

        # Read from PubSub into a PCollection.
        messages = (
            p
            #| beam.io.ReadFromPubSub(topic="projects/pubsub-public-data/topics/taxirides-realtime").with_output_types(bytes))
            | beam.io.ReadFromPubSub(
                subscription=
                "projects/test-anyscale-project-7b2ae2e3/subscriptions/apache-beam-test"
            ).with_output_types(bytes))

        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
        lines | beam.Map(print)

        # Count the occurrences of each word.
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        counts = (lines
                  | 'split' >>
                  (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
                  | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
                  | beam.WindowInto(window.FixedWindows(15, 0))
                  | 'group' >> beam.GroupByKey()
                  | 'count' >> beam.Map(count_ones))

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '%s: %d' % (word, count)

        output = (
            counts
            | 'format' >> beam.Map(format_result)
            | 'encode' >>
            beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

        # Write to PubSub.
        # pylint: disable=expression-not-assigned
        #output | beam.io.WriteToPubSub(known_args.output_topic)
        #output | beam.Map(print)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
