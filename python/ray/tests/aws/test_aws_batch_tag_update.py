import threading
import time
import unittest
from unittest import mock

import pytest

from ray.autoscaler._private.aws.node_provider import AWSNodeProvider
from ray.autoscaler._private.aws.node_provider import TAG_BATCH_DELAY


def mock_create_tags(provider, batch_updates):
    # Increment batches sent.
    provider.batch_counter += 1
    # Increment tags updated.
    provider.tag_update_counter += sum(
        len(batch_updates[x]) for x in batch_updates)


def batch_test(num_threads, delay):
    """Run AWSNodeProvider.set_node_tags in several threads, with a
    specified delay between thread launches.

    Return the number of batches of tag updates and the number of tags
    updated.
    """
    with mock.patch("ray.autoscaler._private.aws.node_provider.make_ec2_client"
                    ), mock.patch.object(AWSNodeProvider, "_create_tags",
                                         mock_create_tags):
        provider = AWSNodeProvider(
            provider_config={"region": "nowhere"}, cluster_name="default")
        provider.batch_counter = 0
        provider.tag_update_counter = 0
        provider.tag_cache = {str(x): {} for x in range(num_threads)}

        threads = []
        for x in range(num_threads):
            thread = threading.Thread(
                target=provider.set_node_tags, args=(str(x), {
                    "foo": "bar"
                }))
            threads.append(thread)

        for thread in threads:
            thread.start()
            time.sleep(delay)
        for thread in threads:
            thread.join()

        return provider.batch_counter, provider.tag_update_counter


class TagBatchTest(unittest.TestCase):
    def test_concurrent(self):
        num_threads = 100
        batches_sent, tags_updated = batch_test(num_threads, delay=0)
        self.assertLess(batches_sent, num_threads / 10)
        self.assertEqual(tags_updated, num_threads)

    def test_serial(self):
        num_threads = 5
        long_delay = TAG_BATCH_DELAY * 1.2
        batches_sent, tags_updated = batch_test(num_threads, delay=long_delay)
        self.assertEqual(batches_sent, num_threads)
        self.assertEqual(tags_updated, num_threads)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
