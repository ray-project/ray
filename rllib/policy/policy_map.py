from collections import deque
import pickle

from ray.rllib.utils.annotations import override


class PolicyMap(dict):
    """Maps policy IDs to Policy objects.

    Thereby, keeps n policies in memory and - when capacity is reached -
    writes the least recently used to disk. This allows adding 100s of
    policies to a Trainer for league-based setups w/o running out of memory.
    """
    def __init__(self, capacity=None, path=None, policy_config=None):
        """Initializes a PolicyMap instance.

        Args:
            maxlen (int): The maximum number of policies to hold in memory.
                The least used ones are written to disk/S3 and retrieved
                when needed.
            path (str):
        """
        super().__init__()

        # Dictionary of keys that may be looked up (cached or not).
        self.valid_keys = set()
        # The actual cache with the in-memory policy objects.
        self.cache = {}
        # The doubly-linked list holding the currently in-memory objects.
        self.deque = deque(maxlen=capacity or 10)
        # The file path where to store overflowing policies.
        self.path = path or "."
        # The core config to use. Each single policy's config override is
        # added on top of this.
        self.policy_config = policy_config or {}

    @override(dict)
    def __getitem__(self, item):
        # Never seen this key -> Error.
        if item not in self.valid_keys:
            raise KeyError(f"'{item}' not a valid key!")

        # Item already in cache -> Rearrange deque (least recently used) and
        # return.
        if item in self.cache:
            self.deque.remove(item)
            self.deque.append(item)
        # Item not currently in cache -> Get from disk and - if at capacity -
        # remove leftmost one.
        else:
            self._read_from_disk(policy_id=item)

        return self.cache[item]

    @override(dict)
    def __setitem__(self, key, value):
        # Item already in cache -> Rearrange deque (least recently used).
        if key in self.cache:
            self.deque.remove(key)
            self.deque.append(key)
            self.cache[key] = value
        # Item not currently in cache -> store new value and - if at capacity -
        # remove leftmost one.
        else:
            # Cache at capacity -> Drop leftmost item.
            if len(self.deque) == self.deque.maxlen:
                self._write_to_disk()
            self.deque.append(key)
            self.cache[key] = value
        self.valid_keys.add(key)

    def _write_to_disk(self):
        """Writes least-recently used policy to disk and rearranges cache.
        """
        delkey = self.deque.popleft()
        policy = self.cache[delkey]
        policy_state = policy.get_state()
        del self.cache[delkey]
        with open(self.path + "/" + delkey + ".policy.pkl", "wb") as f:
            pickle.dump(policy_state, file=f)

    def _read_from_disk(self, policy_id):
        """Reads a policy ID from disk and re-adds it to the cache.
        """
        # Make sure this policy ID is not in the cache right now.
        assert policy_id not in self.cache
        # Read policy state from disk.
        with open(self.path + "/" + policy_id + ".policy.pkl", "r") as f:
            policy_state = pickle.load(f)
        # Create policy object (from its spec: cls, obs-space, act-space, config).
        policy = policy_state["policy_spec"][0](*policy_state["policy_spec"][1:])
        # Restore policy's state.
        policy.set_state(policy_state)
        self.cache[policy_id] = policy

        # Cache at capacity -> Drop leftmost item.
        if len(self.deque) == self.deque.maxlen:
            self._write_to_disk()
        # Add policy ID to deque.
        self.deque.append(policy_id)


cache = PolicyMap(capacity=5)
cache["a"] = 1
cache["b"] = 2
print(cache["a"])
cache["c"] = 3
cache["d"] = 4
cache["e"] = 5
cache["f"] = 6
print(cache["a"])
print(cache["f"])
print(cache["b"])
cache["g"] = 7
cache["h"] = 8
print(cache["c"])
