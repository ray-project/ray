Task State: Definitions & Transition Diagram
============================================

A task can be in one of the following states:

- **Placeable**: the task is ready to be placed at the node where is going to be
  executed. This can be either local or a remote node. The decision is based on
  resource availability (the location and size of the task's arguments are
  ignore). If the local node has enough resources to satisfy task's demand, then
  the task is placed locally, otherwise is forwarded to another node.

- **WaitForActorCreation**: an actor method (task) is waiting for its actor to get
  instantiated. Once the actor is created, the task transitions into the
  waiting state, if the actor is local, or it is forwarded to the remote machine
  running the actor.

- **Waiting**: the task is waiting for its argument dependencies to be satisfied,
  i.e., for its arguments to be transferred to the local object store.

- **Ready**: the task is ready to run, that is, all task's arguments are in the
  local object store.

- **Running**: the task has been dispatched and it is running on a local
  worker/actor.

- **Blocked**: the task is being blocked as some data objects it depends on are not
  available, e.g., because the task has launched another task and it waits
  for the results, ore because of failures.

::

         forward
          ------
         |      |   resource          arguments        actor/worker
         |      v  available            local             available
         Placeable ----------> Waiting --------> Ready ---------> Running
          |     ^                 ^                                |   ^
    actor |     | actor           | actor                   worker |   | worker
  created |     | created         | created                blocked |   | unblocked
          v     | (remote)        | (local)                        v   |
     WaitForActorCreation---------                                Blocked
