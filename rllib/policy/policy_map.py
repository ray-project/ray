from collections import deque
import threading
from typing import Dict, Optional, Set

import ray
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.threading import with_lock
from ray.util.annotations import PublicAPI

tf1, tf, tfv = try_import_tf()


@PublicAPI(stability="beta")
class PolicyMap(dict):
    """Maps policy IDs to Policy objects.

    Thereby, keeps n policies in memory and - when capacity is reached -
    writes the least recently used to disk. This allows adding 100s of
    policies to a Algorithm for league-based setups w/o running out of memory.
    """

    def __init__(
        self,
        *,
        capacity: Optional[int] = None,
        policies_swappable: bool = False,
        # Deprecated args.
        worker_index=None,
        num_workers=None,
        policy_config=None,
        session_creator=None,
        seed=None,
    ):
        """Initializes a PolicyMap instance.

        Args:
            capacity: The maximum number of policies to hold in memory.
                The least used ones are written to disk/S3 and retrieved
                when needed.
            policies_swappable: Whether all Policy objects in this map can be
                "swapped out" by a simple `s = A.get_state(); B.set_state(s)`,
                where `A` and `B` are policy instances in this map.
        """
        if policy_config is not None:
            deprecation_warning(
                old="PolicyMap(policy_config=..)",
                error=True,
            )

        super().__init__()

        if any(
            i is not None
            for i in [policy_config, worker_index, num_workers, session_creator, seed]
        ):
            deprecation_warning(
                old="PolicyMap([deprecated args]...)",
                new="PolicyMap(capacity=..., policies_swappable=...)",
                error=False,
            )

        self.policies_swappable = policies_swappable

        # Dictionary of keys that may be looked up (cached or not).
        self.valid_keys: Set[str] = set()
        # The actual cache with the in-memory policy objects.
        self.cache: Dict[str, Policy] = {}
        # The doubly-linked list holding the currently in-memory objects.
        self.deque = deque(maxlen=capacity or 100)

        # Lock used for locking some methods on the object-level.
        # This prevents possible race conditions when accessing the map
        # and the underlying structures, like self.deque and others.
        self._lock = threading.RLock()

        # Ray object store references to the stashed Policy states.
        self._policy_state_refs = {}

        from ray.util.timer import _Timer
        from collections import defaultdict
        self.timers = defaultdict(_Timer)

    @with_lock
    @override(dict)
    def __getitem__(self, item):
        with self.timers["getitem"]:
            # Never seen this key -> Error.
            if item not in self.valid_keys:
                raise KeyError(
                    f"PolicyID '{item}' not found in this PolicyMap! "
                    f"IDs stored in this map: {self.valid_keys}."
                )

            # Item already in cache -> Rearrange deque (promote `item` to
            # "most recently used") and return it.
            if item in self.cache:
                with self.timers["getitem->cached"]:
                    self.deque.remove(item)
                    self.deque.append(item)
                    return self.cache[item]

            # Item not currently in cache -> Get from stash and - if at capacity -
            # remove leftmost one.
            else:
                with self.timers["getitem->NON-cached"]:
                    assert item in self._policy_state_refs
                    with self.timers["getitem->NON-cached->ray.get"]:
                        policy_state = ray.get(self._policy_state_refs[item])

                    # All our policies have same NN-architecture (are "swappable").
                    # -> Get least recently used one, stash its state to disk and load
                    # new policy's state into that one. This way, we save the costly
                    # re-creation step.
                    if self.policies_swappable and len(self.deque) == self.deque.maxlen:
                        with self.timers["getitem->NON-cached->stash-least-used"]:
                            old_policy_id = self.deque[0]
                            policy = self.cache[old_policy_id]
                            self._stash_least_used_policy()
                            self.cache[item] = policy
                        with self.timers["getitem->NON-cached->set-state"]:
                            # Restore policy's state and return it.
                            policy.set_state(policy_state)
                    # Policies are different or we are not at capacity:
                    # Have to (re-)create new policy here.
                    else:
                        self._stash_least_used_policy()
                        # Create policy object (from its spec: cls, obs-space, act-space,
                        # config).
                        policy = Policy.from_state(policy_state)
                        self.cache[item] = policy
                    with self.timers["getitem->NON-cached->deque.append"]:
                        self.deque.append(item)
                    return policy

    @with_lock
    @override(dict)
    def __setitem__(self, key, value):
        with self.timers["setitem"]:
            # Item already in cache -> Rearrange deque.
            if key in self.cache:
                self.deque.remove(key)

            # Item not currently in cache -> store new value and - if at capacity -
            # remove leftmost one.
            else:
                # Cache at capacity -> Drop leftmost item.
                if len(self.deque) == self.deque.maxlen:
                    self._stash_least_used_policy()

            # Promote `key` to "most recently used".
            self.deque.append(key)

            # Update our cache.
            self.cache[key] = value
            self.valid_keys.add(key)

    @with_lock
    @override(dict)
    def __delitem__(self, key):
        with self.timers["delitem"]:
            # Make key invalid.
            self.valid_keys.remove(key)
            # Remove policy from memory if currently cached.
            if key in self.cache:
                policy = self.cache[key]
                self._close_session(policy)
                del self.cache[key]
            # Remove Ray object store reference so the item gets garbage collected.
            del self._policy_state_refs[key]

    @override(dict)
    def __iter__(self):
        with self.timers["iter"]:
            return iter(self.keys())

    @override(dict)
    def items(self):
        """Iterates over all policies, even the stashed ones."""

        def gen():
            for key in self.valid_keys:
                yield (key, self[key])

        return gen()

    @override(dict)
    def keys(self):
        with self.timers["keys"]:
            self._lock.acquire()
            ks = list(self.valid_keys)
            self._lock.release()

            def gen():
                for key in ks:
                    yield key

            return gen()

    @override(dict)
    def values(self):
        with self.timers["values"]:
            self._lock.acquire()
            vs = [self[k] for k in self.valid_keys]
            self._lock.release()

            def gen():
                for value in vs:
                    yield value

            return gen()

    @with_lock
    @override(dict)
    def update(self, __m, **kwargs):
        with self.timers["update"]:
            for k, v in __m.items():
                self[k] = v
            for k, v in kwargs.items():
                self[k] = v

    @with_lock
    @override(dict)
    def get(self, key):
        with self.timers["get"]:
            if key not in self.valid_keys:
                return None
            return self[key]

    @with_lock
    @override(dict)
    def __len__(self):
        """Returns number of all policies, including the stashed-to-disk ones."""
        with self.timers["len"]:
            return len(self.valid_keys)

    @with_lock
    @override(dict)
    def __contains__(self, item):
        with self.timers["contains"]:
            return item in self.valid_keys

    def _stash_least_used_policy(self):
        """Writes the least-recently used policy's state to the Ray object store.

        Also closes the session - if applicable - of the stashed policy.
        """
        # Get policy's state for writing to disk.
        dropped_policy_id = self.deque.popleft()
        assert dropped_policy_id in self.cache
        policy = self.cache[dropped_policy_id]
        policy_state = policy.get_state()

        # If we don't simply swap out vs an existing policy:
        # Close the tf session, if any.
        if not self.policies_swappable:
            self._close_session(policy)

        # Remove from memory. This will clear the tf Graph as well.
        del self.cache[dropped_policy_id]

        # Store state in Ray object store.
        self._policy_state_refs[dropped_policy_id] = ray.put(policy_state)

    @staticmethod
    def _close_session(policy):
        sess = policy.get_session()
        # Closes the tf session, if any.
        if sess is not None:
            sess.close()
