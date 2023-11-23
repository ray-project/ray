[x] Write benchmark script with Ray DAG
[x] Add "max readers" header to each Plasma buffer. max readers cases:
    - -1: Normal Ray path, created and sealed object, can be read "infinity" times
    - 0: Unsealed object
    - N: Object that can be read at most N times
[x] Implement normal Seal path with shared memory instead of IPC, using max readers header
    - CoreWorker is the "writer"
    - plasma store is the "reader"
[x] Implement max readers=N semaphore pattern and test with ray.put/ray.get
    - Sender CoreWorker actor is the "writer"
    - Receiver CoreWorker actor(s) is the "reader"
    - Implement alternative plasma client put/get calls
[x] VLLM + DAG
    - Implement OutputNode
    - Fix DAG API
[x] Connect actors and driver -> Stephanie
    - Receiver CoreWorker actor selects between normal task queue and signals from shared-memory objects
        - Implement a DependencyWaiter that uses the alternative plasma client to get objects
[x] Serialization edge cases -> Sang
    - [x] Implement data size header to account for different-size objects


[ ] Performance optimization -> Eric
    - [ ] Implement all optimization
    - Specialize Get/Put path -> not working
    - Specialize Execution -> not working
[ ] Input should accept multi args -> Stephanie
[ ] Make it work with multi-reader for tp > 1 -> Stephanie
    - Add assertion when the data size is too big compared to max buffer size
[ ] Verify vllm works with existing setup
[ ] Exception / failure handling with VLLM and accelerated DAG
    [ ] exception -> Stephanie
    [ ] failure handling (maybe use ray.get on the task output) -> Sang


Next week
[ ] Handle different metadata size
[ ] Make it work with Mac or do not build
[ ] Harden shared-memory based Seal
    - [ ] edge case: object aborted/client dies while waiting for seal
    - [ ] fix plasma client ref counting
    - [ ] disable ref counting for real
    - Implement pinning for plasma client objects (so that we don't need the hack to pin in Python memory, which only works for zero-copy objects)
[ ] ray.release on a list causes segfault

Limitation:
- Not working well with regular actor tasks
- Cannot cancel blocked actor task.


Issues:
- ray.get on reused plasma buffer works with numpy but not with bytes (and probably other objects)
