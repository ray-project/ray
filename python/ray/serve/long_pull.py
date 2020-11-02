from collections import defaultdict
import random
from typing import DefaultDict


class LongPullerClient:
    def __init__(self):
        pass


class LongPullerHost:
    def __init__(self):
        self.epoch_ids = defaultdict(lambda: random.randint(0, 1_000_000))

    async def listen_on_changed(self, event_key: str, epoch_id: int):
        pass
