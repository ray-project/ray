import pytest
import time

import ray
from ray.util.pubsub import Publisher, Subscriber, EventBroker

def test_raw_event_broker_api(ray_start_regular):
    class MyMaster(Publisher):
        def __init__(self):
            event_broker = EventBroker.options(name="my_event_broker", lifetime="detached").remote()
            assert ray.get(self.event_broker.ready())
            assert ray.get(event_broker.register.remote("channel1"))

        def trigger_publish(self):
            msg = b"This is a message that needs to be published."
            # published successfully doesn't indicate it's consumed.
            is_published = self.event_broker.publish("channel1", msg)
            assert ray.get(is_published)

        def __on_fetch_latest_messages__(self, channels: list):
            my_latest_msgs = {
                "channel1": b"xxxxx",
                "channel2": b"yyyyy",
                "channel3": b"zzzzz",
            }
            return my_latest_msgs

    class MyWorker(Subscriber):
        def __init__(self):
            assert ray.get(self.event_broker_actor.subscribe("channel1"))

        def __on_message_received__(channel: str, message: bytes):
            # Do my stuff
            return True # True indicates it’s consumed successfully.



if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
