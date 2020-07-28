Actor State: Definitions & Transition Diagram
============================================

An actor can be in one of the following states:

- **DEPENDENCIES_UNREADY**: the actor info ins registered in GCS. But its dependencies are not ready.

- **PENDING_CREATION**: the actor local dependencies are ready. This actor is being created.

- **ALIVE**: the actor is created successfully.

- **RESTARTING**: the actor is dead, now being restarted. After reconstruction finishes,
  the state will become alive again.

- **DEAD**: the actor is already dead and won't be restarted.

::

                                                                         3
   0                            1                       2          ------------->
 ---->DEPENDENCIES_UNREADY-------->PENDING_CREATION-------->ALIVE                RESTARTING
               |                            |                  |   <-------------      |
               |                            |                  |         4             |
               |                            |                  |                       |
             8 |                         7  |                6 |                       | 5
               |                            |                  |                       |
               |                            |                  |                       |
               |                            |                  |                       |
               |                            v                  |                       |
                -------------------------->DEAD<---------------------------------------

- **0**: GCS will create an actor whose state is `DEPENDENCIES_UNREADY`
   when a `RegisterActor` request is received from CoreWorker.

- **1**: GCS will change actor state to `PENDING_CREATION` when a `CreateActor` request is received from CoreWorker.

- **2**: GCS will change actor state to `ALIVE` when a successful `PushNormalTask` reply is received from CoreWorker.

- **3**: GCS will change actor state to `RESTARTING` when the owner worker/node of actor is dead.

- **4**: GCS will change actor state to `ALIVE` when the actor reconstruction is successful.

- **5**: GCS will change actor state to `DEAD` when the actor remaining restarts number is 0.

- **6**: GCS will change actor state to `DEAD` when the owner worker/node/actor is dead.

- **7**: GCS will change actor state to `DEAD` when the owner worker/node/actor is dead.

- **8**: GCS will change actor state to `DEAD` when the owner worker/node/actor is dead.
