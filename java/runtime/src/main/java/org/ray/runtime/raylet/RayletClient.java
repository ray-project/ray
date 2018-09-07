package org.ray.runtime.raylet;

import java.util.List;

import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.runtime.task.TaskSpec;

/**
 * Client to the Raylet backend.
 */
public interface RayletClient {

  void submitTask(TaskSpec task);

  TaskSpec getTask();

  void reconstructObjects(List<UniqueId> objectIds, boolean fetchOnly);

  void notifyUnblocked();

  UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex);

  <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int timeoutMs);

  void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly);
}
