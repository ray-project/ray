import msgspec
import zmq
from msgspec.msgpack import Decoder

import ray


@ray.remote
class KVEventManager:
    """Ray actor for managing KV cache events via ZMQ."""

    def __init__(self):
        """Initialize the KV Event Manager."""
        self.initialize_types()
        self.decoder = Decoder(type=self.KVEventBatch)
        self.last_seq = -1

        self.context = zmq.Context()

        # Set up the main subscription socket
        self.sub = self.context.socket(zmq.SUB)
        self.sub.connect("tcp://localhost:5557")
        topic = "kv-events"
        self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

        # Initialize replay socket
        self.replay = self.context.socket(zmq.REQ)
        self.replay.connect("tcp://localhost:5558")
        self.poller = zmq.Poller()
        self.poller.register(self.replay, zmq.POLLIN)

        print("Listening for KV cache events on topic:", topic)

    def initialize_types(self):
        """
        Store the classes as instance attributes because Ray can't serialize
        StructMeta, used by msgspec.
        """
        from typing import Any, Optional, Union

        class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
            ts: float
            events: list[Any]

        class KVCacheEvent(
            msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True
        ):
            """Base class for all KV cache-related events"""

        class BlockStored(KVCacheEvent):
            block_hashes: list[int]
            parent_block_hash: Optional[int]
            token_ids: list[int]
            block_size: int
            lora_id: Optional[int]

        class BlockRemoved(KVCacheEvent):
            block_hashes: list[int]

        class AllBlocksCleared(KVCacheEvent):
            pass

        class KVEventBatch(EventBatch):
            events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]

        self.EventBatch = EventBatch
        self.KVCacheEvent = KVCacheEvent
        self.BlockStored = BlockStored
        self.BlockRemoved = BlockRemoved
        self.AllBlocksCleared = AllBlocksCleared
        self.KVEventBatch = KVEventBatch

    def process_event(self, event_batch):
        """Process a batch of KV cache events."""
        print(f"Received event batch at {event_batch.ts}:")
        for event in event_batch.events:
            print(f"  - {event}")

    def run(self):
        """Main event loop"""
        while True:
            try:
                if self.sub.poll(50):
                    _, seq_bytes, payload = self.sub.recv_multipart()
                    seq = int.from_bytes(seq_bytes, "big")

                    if self.last_seq >= 0 and seq > self.last_seq + 1:
                        missed = seq - self.last_seq - 1
                        print(
                            f"Missed {missed} messages (last: {self.last_seq}, current: {seq})"
                        )

                        self.replay.send((self.last_seq + 1).to_bytes(8, "big"))

                        while self.poller.poll(timeout=200):
                            seq_bytes, replay_payload = self.replay.recv_multipart()
                            if not replay_payload:
                                # End of replay marker is sent as an empty frame
                                # for the payload
                                break

                            replay_seq = int.from_bytes(seq_bytes, "big")

                            if replay_seq > self.last_seq:
                                event_batch = self.decoder.decode(replay_payload)
                                self.process_event(event_batch)
                                self.last_seq = replay_seq
                                if replay_seq >= seq - 1:
                                    break

                    event_batch = self.decoder.decode(payload)
                    self.process_event(event_batch)
                    self.last_seq = seq

                    # ... do other periodic work or check for shutdown ...

            except KeyboardInterrupt:
                print("Interrupted")
                break
            except Exception as e:
                print("Error decoding message:", e)


def main():
    """Main function for standalone usage."""
    ray.init()

    # Create the named actor
    manager = KVEventManager.options(name="kv_event_manager").remote()

    # Start listening for events
    try:
        ray.get(manager.run.remote())
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
