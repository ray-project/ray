Actor State: Definitions & Transition Diagram
============================================

An actor can be in one of the following states:

- **DEPENDENCIES_UNREADY**: the actor info is registered in GCS. But its dependencies are not ready.

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

- **0**: When GCS receives a `RegisterActor` request from core worker, GCS will persist the actor in database with state `DEPENDENCIES_UNREADY`.

- **1**: When core worker has finished resolving the actor dependencies, it will send a `CreateActor` request to GCS and GCS will update actor state to `PENDING_CREATION` in memory.

- **2**: When core worker has finished actor creation task, it will send a `PushTask` reply to GCS and GCS will persist the actor in database with state `ALIVE`.

- **3**: When GCS detected that the worker/node of actor is dead and the actor remaining restarts number is greater than 0, it will persist the actor in database with state `RESTARTING`.

- **4**: When the actor is successfully reconstructed, GCS will persist the actor in database with state `ALIVE`.

- **5**: If the actor is restarting, GCS detects that its worker or node is dead and the actor remaining restarts number is 0, it will persist the actor in database with state `DEAD`.
  If the actor is not detached, when GCS detected that the owner worker/node/actor is dead, it will persist the actor in database with state `DEAD`.

- **6**: When GCS detected that the actor is dead and the actor remaining restarts number is 0, it will persist the actor in database with state `DEAD`.
  If the actor is not detached, when GCS detected that the owner worker/node/actor is dead, it will persist the actor in database with state `DEAD`.

- **7**: If the actor is not detached, when GCS detected that the owner worker/node/actor is dead, it will persist the actor in database with state `DEAD`.

- **8**: When GCS detected that the actor creator is dead, it will persist the actor in database with state `DEAD`.
  If the actor is not detached, when GCS detected that the owner worker/node/actor is dead, it will persist the actor in database with state `DEAD`.
