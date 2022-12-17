import logging
import os
import time

from ray.util.debug import log_once
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


class _TFRunBuilder:
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
                self._executed = _run_timeline(
                    self.session,
                    self.fetches,
                    self.debug_name,
                    self.feed_dict,
                    os.environ.get("TF_TIMELINE_DIR"),
                )
            except Exception as e:
                logger.exception(
                    "Error fetching: {}, feed_dict={}".format(
                        self.fetches, self.feed_dict
                    )
                )
                raise e
        if isinstance(to_fetch, int):
            return self._executed[to_fetch]
        elif isinstance(to_fetch, list):
            return [self.get(x) for x in to_fetch]
        elif isinstance(to_fetch, tuple):
            return tuple(self.get(x) for x in to_fetch)
        else:
            raise ValueError("Unsupported fetch type: {}".format(to_fetch))


_count = 0


def _run_timeline(sess, ops, debug_name, feed_dict=None, timeline_dir=None):
    if feed_dict is None:
        feed_dict = {}

    if timeline_dir:
        from tensorflow.python.client import timeline

        try:
            run_options = tf1.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
        except AttributeError:
            run_options = None
            # In local mode, tf1.RunOptions is not available, see #26511
            if log_once("tf1.RunOptions_not_available"):
                logger.exception(
                    "Can not run properly run tf timeline in local_mode. "
                    "RLlib will use timeline without "
                    "`options=tf.RunOptions.FULL_TRACE`."
                )
        run_metadata = tf1.RunMetadata()
        start = time.time()
        fetches = sess.run(
            ops, options=run_options, run_metadata=run_metadata, feed_dict=feed_dict
        )
        trace = timeline.Timeline(step_stats=run_metadata.step_stats)
        global _count
        outf = os.path.join(
            timeline_dir,
            "timeline-{}-{}-{}.json".format(debug_name, os.getpid(), _count % 10),
        )
        _count += 1
        trace_file = open(outf, "w")
        logger.info(
            "Wrote tf timeline ({} s) to {}".format(
                time.time() - start, os.path.abspath(outf)
            )
        )
        trace_file.write(trace.generate_chrome_trace_format())
    else:
        if log_once("tf_timeline"):
            logger.info(
                "Executing TF run without tracing. To dump TF timeline traces "
                "to disk, set the TF_TIMELINE_DIR environment variable."
            )
        fetches = sess.run(ops, feed_dict=feed_dict)
    return fetches
