from collections import deque
import os
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

        # The file extension for stashed policies (that are no longer available
        # in-memory but can be reinstated any time from storage).
        self.extension = ".policy.pkl"

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
                self._stash_to_disk()
            self.deque.append(key)
            self.cache[key] = value
        self.valid_keys.add(key)

    @override(dict)
    def __delitem__(self, key):
        # Make key invalid.
        self.valid_keys.remove(key)
        # Remove policy from memory if currently cached.
        if key in self.cache:
            policy = self.cache[key]
            self._close_session(policy)
            del self.cache[key]
        # Remove file associated with the policy, if it exists.
        filename = self.path + "/" + key + self.extension
        if os.path.isfile(filename):
            os.remove(filename)

    @override(dict)
    def __iter__(self):
        return self.keys()

    @override(dict)
    def items(self):
        """Iterates over all policies, even the stashed-to-disk ones."""

        def gen():
            for key in self.valid_keys:
                yield (key, self[key])

        return gen()

    @override(dict)
    def keys(self):
        def gen():
            for key in self.valid_keys:
                yield key

        return gen()

    @override(dict)
    def values(self):
        def gen():
            for key in self.valid_keys:
                yield self[key]

        return gen()

    @override(dict)
    def update(self, __m, **kwargs):
        for k, v in __m.items():
            self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    @override(dict)
    def get(self, key):
        if key not in self.valid_keys:
            return None
        return self[key]

    @override(dict)
    def __len__(self):
        """Returns number of all policies, including the stashed-to-disk ones.
        """
        return len(self.valid_keys)

    @override(dict)
    def __contains__(self, item):
        return item in self.valid_keys

    def _stash_to_disk(self):
        """Writes the least-recently used policy to disk and rearranges cache.

        Also closes the session - if applicable - of the stashed policy.
        """
        # Get least recently used policy (all the way on the left in deque).
        delkey = self.deque.popleft()
        policy = self.cache[delkey]
        # Get its state for writing to disk.
        policy_state = policy.get_state()
        # Add class of Policy to state.
        policy_state["_cls"] = policy.__class__
        # Closes policy's tf session, if any.
        self._close_session(policy)
        # Remove from memory.
        # TODO: (sven) This should clear the tf Graph as well, if the Trainer
        #  would not hold parts of the graph (e.g. in tf multi-GPU setups).
        del self.cache[delkey]
        # Write state to disk.
        with open(self.path + "/" + delkey + self.extension, "wb") as f:
            pickle.dump(policy_state, file=f)

    def _read_from_disk(self, policy_id):
        """Reads a policy ID from disk and re-adds it to the cache.
        """
        # Make sure this policy ID is not in the cache right now.
        assert policy_id not in self.cache
        # Read policy state from disk.
        with open(self.path + "/" + policy_id + self.extension, "r") as f:
            policy_state = pickle.load(f)
        # Create policy object (from its spec: cls, obs-space, act-space,
        # config).
        policy = policy_state["policy_spec"][0](
            *policy_state["policy_spec"][1:])
        # Restore policy's state.
        policy.set_state(policy_state)
        self.cache[policy_id] = policy

        # Cache at capacity -> Drop leftmost item.
        if len(self.deque) == self.deque.maxlen:
            self._stash_to_disk()
        # Add policy ID to deque.
        self.deque.append(policy_id)

    def _close_session(self, policy):
        sess = policy.get_session()
        # Closes the tf session, if any.
        if sess is not None:
            sess.close()
