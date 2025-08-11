from ray.llm._internal.serve.request_router.prefix_aware.prefix_aware_router import (
    PrefixCacheAffinityRouter as _PrefixCacheAffinityRouter,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class PrefixCacheAffinityRouter(_PrefixCacheAffinityRouter):
    """A request router that is aware of the KV cache.

    This router optimizes request routing by considering KV cache locality,
    directing requests with similar prefixes to the same replica to improve
    cache hit rates.

    The internal policy is this (it may change in the future):

    1. Mixes between three strategies to balance prefix cache hit rate and load
    balancing:
       - When load is balanced (queue length difference < threshold), it
       selects replicas with the highest prefix match rate for the input text
       - When load is balanced but match rate is below 10%, it falls back to
       the smallest tenants (i.e. the replica with the least kv cache)
       - When load is imbalanced, it uses the default Power of Two selection

    2. Maintains a prefix tree to track which replicas have processed similar
    inputs:
       - Inserts prompt text into the prefix tree after routing
       - Uses this history to inform future routing decisions

    Parameters:
        imbalanced_threshold: The threshold for considering the load imbalanced.
        match_rate_threshold: The threshold for considering the match rate.
        do_eviction: Whether to do eviction.
        eviction_threshold_chars: Number of characters in the tree to trigger
            eviction.
        eviction_target_chars: Number of characters in the tree to target for
            eviction.
        eviction_interval_secs: How often (in seconds) to run the eviction
        policy.
    """

    pass
