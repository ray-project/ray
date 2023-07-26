from collections import deque
import threading
from typing import Dict, Set
import logging

import ray
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.typing import PolicyID
from ray.util.annotations import PublicAPI

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


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
        capacity: int = 100,
        policy_states_are_swappable: bool = False,
        # Deprecated args.
        worker_index=None,
        num_workers=None,
        policy_config=None,
        session_creator=None,
        seed=None,
    ):
        """Initializes a PolicyMap instance.

        Args:
            capacity: The size of the Policy object cache. This is the maximum number
                of policies that are held in RAM memory. When reaching this capacity,
                the least recently used Policy's state will be stored in the Ray object
                store and recovered from there when being accessed again.
            policy_states_are_swappable: Whether all Policy objects in this map can be
                "swapped out" via a simple `state = A.get_state(); B.set_state(state)`,
                where `A` and `B` are policy instances in this map. You should set
                this to True for significantly speeding up the PolicyMap's cache lookup
                times, iff your policies all share the same neural network
                architecture and optimizer types. If True, the PolicyMap will not
                have to garbage collect old, least recently used policies, but instead
                keep them in memory and simply override their state with the state of
                the most recently accessed one.
                For example, in a league-based training setup, you might have 100s of
                the same policies in your map (playing against each other in various
                combinations), but all of them share the same state structure
                (are "swappable").
        """
        if policy_config is not None:
            deprecation_warning(
                old="PolicyMap(policy_config=..)",
                error=True,
            )

        super().__init__()

        self.capacity = capacity

        if any(
            i is not None
            for i in [policy_config, worker_index, num_workers, session_creator, seed]
        ):
            deprecation_warning(
                old="PolicyMap([deprecated args]...)",
                new="PolicyMap(capacity=..., policy_states_are_swappable=...)",
                error=False,
            )

        self.policy_states_are_swappable = policy_states_are_swappable

        # The actual cache with the in-memory policy objects.
        self.cache: Dict[str, Policy] = {}

        # Set of keys that may be looked up (cached or not).
        self._valid_keys: Set[str] = set()
        # The doubly-linked list holding the currently in-memory objects.
        self._deque = deque()

        # Ray object store references to the stashed Policy states.
        self._policy_state_refs = {}

        # Lock used for locking some methods on the object-level.
        # This prevents possible race conditions when accessing the map
        # and the underlying structures, like self._deque and others.
        self._lock = threading.RLock()

    @with_lock
    @override(dict)
    def __getitem__(self, item: PolicyID):
        # Never seen this key -> Error.
        if item not in self._valid_keys:
            raise KeyError(
                f"PolicyID '{item}' not found in this PolicyMap! "
                f"IDs stored in this map: {self._valid_keys}."
            )

        # Item already in cache -> Rearrange deque (promote `item` to
        # "most recently used") and return it.
        if item in self.cache:
            self._deque.remove(item)
            self._deque.append(item)
            return self.cache[item]

        # Item not currently in cache -> Get from stash and - if at capacity -
        # remove leftmost one.
        if item not in self._policy_state_refs:
            raise AssertionError(
                f"PolicyID {item} not found in internal Ray object store cache!"
            )
        policy_state = ray.get(self._policy_state_refs[item])

        policy = None
        # We are at capacity: Remove the oldest policy from deque as well as the
        # cache and return it.
        if len(self._deque) == self.capacity:
            policy = self._stash_least_used_policy()

        # All our policies have same NN-architecture (are "swappable").
        # -> Load new policy's state into the one that just got removed from the cache.
        # This way, we save the costly re-creation step.
        if policy is not None and self.policy_states_are_swappable:
            logger.debug(f"restoring policy: {item}")
            policy.set_state(policy_state)
        else:
            logger.debug(f"creating new policy: {item}")
            policy = Policy.from_state(policy_state)

        self.cache[item] = policy
        # Promote the item to most recently one.
        self._deque.append(item)

        return policy

    @with_lock
    @override(dict)
    def __setitem__(self, key: PolicyID, value: Policy):
        # Item already in cache -> Rearrange deque.
        if key in self.cache:
            self._deque.remove(key)

        # Item not currently in cache -> store new value and - if at capacity -
        # remove leftmost one.
        else:
            # Cache at capacity -> Drop leftmost item.
            if len(self._deque) == self.capacity:
                self._stash_least_used_policy()

        # Promote `key` to "most recently used".
        self._deque.append(key)

        # Update our cache.
        self.cache[key] = value
        self._valid_keys.add(key)

    @with_lock
    @override(dict)
    def __delitem__(self, key: PolicyID):
        # Make key invalid.
        self._valid_keys.remove(key)
        # Remove policy from deque if contained
        if key in self._deque:
            self._deque.remove(key)
        # Remove policy from memory if currently cached.
        if key in self.cache:
            policy = self.cache[key]
            self._close_session(policy)
            del self.cache[key]
        # Remove Ray object store reference (if this ID has already been stored
        # there), so the item gets garbage collected.
        if key in self._policy_state_refs:
            del self._policy_state_refs[key]

    @override(dict)
    def __iter__(self):
        return iter(self.keys())

    @override(dict)
    def items(self):
        """Iterates over all policies, even the stashed ones."""

        def gen():
            for key in self._valid_keys:
                yield (key, self[key])

        return gen()

    @override(dict)
    def keys(self):
        """Returns all valid keys, even the stashed ones."""
        self._lock.acquire()
        ks = list(self._valid_keys)
        self._lock.release()

        def gen():
            for key in ks:
                yield key

        return gen()

    @override(dict)
    def values(self):
        """Returns all valid values, even the stashed ones."""
        self._lock.acquire()
        vs = [self[k] for k in self._valid_keys]
        self._lock.release()

        def gen():
            for value in vs:
                yield value

        return gen()

    @with_lock
    @override(dict)
    def update(self, __m, **kwargs):
        """Updates the map with the given dict and/or kwargs."""
        for k, v in __m.items():
            self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    @with_lock
    @override(dict)
    def get(self, key: PolicyID):
        """Returns the value for the given key or None if not found."""
        if key not in self._valid_keys:
            return None
        return self[key]

    @with_lock
    @override(dict)
    def __len__(self) -> int:
        """Returns number of all policies, including the stashed-to-disk ones."""
        return len(self._valid_keys)

    @with_lock
    @override(dict)
    def __contains__(self, item: PolicyID):
        return item in self._valid_keys

    @override(dict)
    def __str__(self) -> str:
        # Only print out our keys (policy IDs), not values as this could trigger
        # the LRU caching.
        return (
            f"<PolicyMap lru-caching-capacity={self.capacity} policy-IDs="
            f"{list(self.keys())}>"
        )

    def _stash_least_used_policy(self) -> Policy:
        """Writes the least-recently used policy's state to the Ray object store.

        Also closes the session - if applicable - of the stashed policy.

        Returns:
            The least-recently used policy, that just got removed from the cache.
        """
        # Get policy's state for writing to object store.
        dropped_policy_id = self._deque.popleft()
        assert dropped_policy_id in self.cache
        policy = self.cache[dropped_policy_id]
        policy_state = policy.get_state()

        # If we don't simply swap out vs an existing policy:
        # Close the tf session, if any.
        if not self.policy_states_are_swappable:
            self._close_session(policy)

        # Remove from memory. This will clear the tf Graph as well.
        del self.cache[dropped_policy_id]

        # Store state in Ray object store.
        self._policy_state_refs[dropped_policy_id] = ray.put(policy_state)

        # Return the just removed policy, in case it's needed by the caller.
        return policy

    @staticmethod
    def _close_session(policy: Policy):
        sess = policy.get_session()
        # Closes the tf session, if any.
        if sess is not None:
            sess.close()
