package org.ray.runtime.raylet;

import java.util.List;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskSpec;

/**
 * Client to the Raylet backend.
 */
public interface RayletClient {

  void submitTask(TaskSpec task);

  TaskSpec getTask();

  <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int
      timeoutMs, TaskId currentTaskId);

  void freePlasmaObjects(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks);

  UniqueId prepareCheckpoint(ActorId actorId);

  void notifyActorResumedFromCheckpoint(ActorId actorId, UniqueId checkpointId);

  void setResource(String resourceName, double capacity, UniqueId nodeId);

  void destroy();
}
