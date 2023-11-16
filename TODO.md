[x] Write benchmark script with Ray DAG
[x] Add "max readers" header to each Plasma buffer. max readers cases:
    - -1: Normal Ray path, created and sealed object, can be read "infinity" times
    - 0: Unsealed object
    - N: Object that can be read at most N times
[x] Implement normal Seal path with shared memory instead of IPC, using max readers header
    - CoreWorker is the "writer"
    - plasma store is the "reader"
[ ] Harden shared-memory based Seal
    - [ ] edge case: object aborted/client dies while waiting for seal
    - [ ] fix plasma client ref counting
    - [ ] disable ref counting for real
[x] Implement max readers=N semaphore pattern and test with ray.put/ray.get
    - Sender CoreWorker actor is the "writer"
    - Receiver CoreWorker actor(s) is the "reader"
    - Implement alternative plasma client put/get calls
[ ] Serialization edge cases
    - Implement pinning for plasma client objects (so that we don't need the hack to pin in Python memory, which only works for zero-copy objects)
    - Implement data size header to account for different-size objects
[ ] Connect actors and driver
    - Receiver CoreWorker actor selects between normal task queue and signals from shared-memory objects
        - Implement a DependencyWaiter that uses the alternative plasma client to get objects


Issues:
- ray.get on reused plasma buffer works with numpy but not with bytes (and probably other objects)
