from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import time

import tensorflow as tf
from tensorflow.python.client import timeline

logger = logging.getLogger(__name__)


class TFRunBuilder(object):
    """Used to incrementally build up a TensorFlow run.

    This is particularly useful for batching ops from multiple different
    policies in the multi-agent setting.
    """

    def __init__(self, session, debug_name):
        self.session = session
        self.debug_name = debug_name
        self.feed_dict = {}
        self.fetches = []
        self._executed = None

    def add_feed_dict(self, feed_dict):
        assert not self._executed
        for k in feed_dict:
            if k in self.feed_dict:
                raise ValueError("Key added twice: {}".format(k))
        self.feed_dict.update(feed_dict)

    def add_fetches(self, fetches):
        assert not self._executed
        base_index = len(self.fetches)
        self.fetches.extend(fetches)
        return list(range(base_index, len(self.fetches)))

    def get(self, to_fetch):
        if self._executed is None:
            try:
                self._executed = run_timeline(
                    self.session, self.fetches, self.debug_name,
                    self.feed_dict, os.environ.get("TF_TIMELINE_DIR"))
            except Exception:
                raise ValueError("Error fetching: {}, feed_dict={}".format(
                    self.fetches, self.feed_dict))
        if isinstance(to_fetch, int):
            return self._executed[to_fetch]
        elif isinstance(to_fetch, list):
            return [self.get(x) for x in to_fetch]
        elif isinstance(to_fetch, tuple):
            return tuple(self.get(x) for x in to_fetch)
        else:
            raise ValueError("Unsupported fetch type: {}".format(to_fetch))


_count = 0


def run_timeline(sess, ops, debug_name, feed_dict={}, timeline_dir=None):
    if timeline_dir:
        run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
        run_metadata = tf.RunMetadata()
        start = time.time()
        fetches = sess.run(
            ops,
            options=run_options,
            run_metadata=run_metadata,
            feed_dict=feed_dict)
        trace = timeline.Timeline(step_stats=run_metadata.step_stats)
        global _count
        outf = os.path.join(
            timeline_dir, "timeline-{}-{}-{}.json".format(
                debug_name, os.getpid(), _count))
        _count += 1
        trace_file = open(outf, "w")
        logger.info("Wrote tf timeline ({} s) to {}".format(
            time.time() - start, os.path.abspath(outf)))
        trace_file.write(trace.generate_chrome_trace_format())
    else:
        fetches = sess.run(ops, feed_dict=feed_dict)
    return fetches
