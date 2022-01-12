Task State: Definitions & Transition Diagram
============================================

A task can be in one of the following states:

- **Placeable**: the task is ready to be assigned to a node (either a local or a
  remote node). The decision is based on resource availability (the location and
  size of the task's arguments are currently ignored). If the local node has
  enough resources to satisfy task's demand, then the task is placed locally,
  otherwise it is forwarded to another node. This placement decision is not
  final. The task can later be spilled over to another node.

- **WaitForActorCreation**: an actor method (task) is waiting for its actor to get
  instantiated. Once the actor is created, the task will be forwarded to the
  remote machine running the actor.

- **Waiting**: the task is waiting for its argument dependencies to be satisfied,
  i.e., for its arguments to be transferred to the local object store.

- **Ready**: the task is ready to run, that is, all task's arguments are in the
  local object store.

- **Running**: the task has been dispatched and it is running on a local
  worker/actor.

- **Blocked**: the task is being blocked as some data objects it depends on are not
  available, e.g., because the task has launched another task and is waiting
  for the results.

- **Infeasible:** the task has resource requirements that are not satisfied by
  any machine.

::

                                                    forward
                            +------------------------------+----------------------------------+
                            |                              |                                  |
                    +-------v--------+             +-------+--------+                  +------+--------+
                    |                |   resource  |                | arguments local  |               |
                    |                +------------->                +------------------>               |
         +---------->   Placeable    |   available |     Waiting    |                  |     Ready     |
         |          |                |             |                | local arg evicted|               |
         |          |                <------+      |                <------------------+               |
         |          +---+------+-----+      |      +--------^-------+                  +-+-----+-------+
         |              |      |            |               |                            ^     |
     node with          |      |            |               |                            |     |
  resources joins       |      |            |               +----------------------------+     |
         |          resources  |     actor created          |                              actor/worker
         |          infeasible |        (remote)       actor created                        available    
         |              |     actor         |             (local)                              |
         |              |     created       |               |                                  |
 +-------+---------+    |      |        +---+---------------+------------+        +------------v--+               +---------------+
 |                 |    |      |        |                                |        |               | worker blocked|               |
 |                 |    |      |        |                                |        |               +--------------->               |
 |    Infeasible   +<---+      +------->+      WaitForActorCreation      |        |    Running    |               |    Blocked    |
 |                 |                    |                                |        |               |  worker       |               |
 |                 |                    |                                |        |               <---------------+               |
 +-----------------+                    +--------------------------------+        +---------------+  unblocked    +---------------+