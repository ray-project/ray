from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class TFRunBuilder(object):
    """Used to incrementally build up a TensorFlow run.

    This is particularly useful for batching ops from multiple different
    policies in the multi-agent setting.
    """

    def __init__(self, session):
        self.session = session
        self.feed_dict = {}
        self.fetches = []
        self._executed = None

    def add_feed_dict(self, feed_dict):
        assert not self._executed
        for k in feed_dict:
            assert k not in self.feed_dict
        self.feed_dict.update(feed_dict)

    def add_fetches(self, fetches):
        assert not self._executed
        base_index = len(self.fetches)
        self.fetches.extend(fetches)
        return list(range(base_index, len(self.fetches)))

    def get(self, to_fetch):
        if self._executed is None:
            try:
                self._executed = self.session.run(
                    self.fetches, feed_dict=self.feed_dict)
            except Exception as e:
                print("Error fetching: {}, feed_dict={}".format(
                    self.fetches, self.feed_dict))
                raise e
        if isinstance(to_fetch, int):
            return self._executed[to_fetch]
        elif isinstance(to_fetch, list):
            return [self.get(x) for x in to_fetch]
        elif isinstance(to_fetch, tuple):
            return tuple(self.get(x) for x in to_fetch)
        else:
            raise ValueError("Unsupported fetch type: {}".format(to_fetch))
