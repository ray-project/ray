Redis Memory Management (Experimental)
======================================

Ray stores metadata associated with tasks and objects in one or more Redis
servers, as described in `An Overview of the Internals
<internals-overview.html>`_.  Applications that are long-running or have high
task/object generation rate could risk high memory pressure, potentially leading
to out-of-memory (OOM) errors.

In Ray `0.6.1+` Redis shards can be configured to LRU evict task and object
metadata by setting ``redis_max_memory_bytes`` when starting Ray. This
supercedes the previously documented flushing functionality.

Note that profiling is disabled when ``redis_max_memory_bytes`` is set. This is
because profiling data cannot be LRU evicted.
