package org.ray.spi;

import java.util.List;
import org.ray.api.id.UniqueId;
import org.ray.spi.model.TaskSpec;

/**
 * Provides core functionalities of local scheduler.
 */
public interface LocalSchedulerLink {

  void submitTask(TaskSpec task);

  TaskSpec getTask();

  void markTaskPutDependency(UniqueId taskId, UniqueId objectId);

  void reconstructObject(UniqueId objectId, boolean fetchOnly);

  void reconstructObjects(List<UniqueId> objectIds, boolean fetchOnly);

  void notifyUnblocked();

  UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex);

  List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns);
}
