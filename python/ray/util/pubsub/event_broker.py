import ray
from ray.actor import ActorHandle

from channel_table import ChannelTable

class PendingMessageEvent:
    def __init__(self, message: bytes, subscriber: ActorHandle):
        self._subscriber = subscriber
        self._message = message
        self._num_retries = 0


@ray.remote
class EventBroker:
    def __init__(self):
        self._channel_table: ChannelTable = ChannelTable()
        self._pending_events: list = []

    def ready(self):
        return True
    
    def register(self, channel: str, publisher: ActorHandle):
        ok = self._channel_table.register_publisher(channel, publisher)
        return ok

    def publish(self, channel: str, message: bytes):
        self._channel_table.add_message(channel, message)
        subscribers = self._channel_table.get_subscribers(channel)
        for subscriber in subscribers:
            object_ref = subscriber.__on_message_received__.remote(channel, message)
            pending_event = PendingMessageEvent(message, subscriber)
            self._pending_events.append(pending_event)
        return True


    def subscribe(self, channel: str, subscriber: ActorHandle):
        ok = self._channel_table.register_subsciber(channel, subscriber)
        return ok 
