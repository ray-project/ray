Actor State: Definitions & Transition Diagram
============================================

An actor can be in one of the following states:

- **DEPENDENCIES_UNREADY**: The actor info is registered in GCS. But its dependencies are not ready.

- **PENDING_CREATION**: The actor local dependencies are ready. This actor is being created.

- **ALIVE**: The actor is created successfully.

- **RESTARTING**: The actor is dead, now being restarted. After reconstruction finishes,
  the state will become alive again.

- **DEAD**: The actor is already dead and won't be restarted.

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

- **2**: When core worker has finished actor creation task, it will send a `PushTask` reply to GCS and GCS will update actor state to `ALIVE` in database.

- **3**: When GCS detects that the worker/node of an actor is dead and the actor's remaining restarts number is greater than 0, it will update actor state to `RESTARTING` in database.

- **4**: When the actor is successfully reconstructed, GCS will update its state to `ALIVE` in the database.

- **5**:

  1) If the actor is restarting, GCS detects that its worker or node is dead and its remaining restarts number is 0, it will update its state to `DEAD` in database.

  2) If an actor is non-detached, when GCS detects that its owner is dead, it will update its state to `DEAD` in the database.

- **6**:

  1) When GCS detected that an actor is dead and its remaining restarts number is 0, it will update its state to `DEAD` in database.

  2) If the actor is non-detached, when GCS detects that its owner is dead, it will update its state to `DEAD` in the database.

- **7**: If the actor is non-detached, when GCS detects that its owner is dead, it will update its state to `DEAD` in the database.

- **8**:

  1) For both detached and non-detached actors, when GCS detects that an actor's creator is dead, it will update its state to `DEAD`. Because in this case, the actor can never be created.

  2) If the actor is non-detached, when GCS detects that its owner is dead, it will update its state to `DEAD` in the database.
