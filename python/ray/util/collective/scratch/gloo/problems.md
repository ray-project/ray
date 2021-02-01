# Problems

- reduce: non-root rank recv something.

        The non-root recvbuf will recieves some data.
        Example: root is 0. But recvbuf of rank 1 shouldn't change.
        (pid=5479) rank 0 sends [[1. 2. 3.]
        (pid=5479)              [1. 2. 3.]],
        (pid=5479)  receives [[2. 4. 6.]
        (pid=5479)           [2. 4. 6.]]
        (pid=5478) rank 1 sends [[1. 2. 3.]
        (pid=5478)              [1. 2. 3.]],
        (pid=5478)  receives [[0. 0. 0.]
        (pid=5478)           [0. 4. 6.]]

- The differences between `broadcast` and `bcast`