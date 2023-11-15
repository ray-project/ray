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
[ ] Implement max readers=N semaphore pattern and test with ray.put/ray.get
    - Sender CoreWorker actor is the "writer"
    - Receiver CoreWorker actor(s) is the "reader"
    - Implement alternative plasma client put/get calls
[ ] Connect actors and driver
    - Receiver CoreWorker actor selects between normal task queue and signals from shared-memory objects
        - Implement a DependencyWaiter that uses the alternative plasma client to get objects



writer gets buffer with 0
writer writes
writer sets to max readers=2
reader 1 reads, decrement to max readers=1
reader 2 reads, decrement to max readers=0
writer can write again


ray.put flow
------------
client -> Create IPC -> store
store allocates object -> IPC reply -> client
client copies data into buffer
client uses shared-memory to Seal -> store gets signal
