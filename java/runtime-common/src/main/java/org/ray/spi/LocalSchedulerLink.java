package org.ray.spi;

import java.util.List;
import org.ray.api.UniqueID;
import org.ray.spi.model.TaskSpec;

/**
 * Provides core functionalities of local scheduler.
 */
public interface LocalSchedulerLink {

  void submitTask(TaskSpec task);

  TaskSpec getTaskTodo();

  void markTaskPutDependency(UniqueID taskId, UniqueID objectId);

  void reconstructObject(UniqueID objectId, boolean fetchOnly);

  void reconstructObjects(List<UniqueID> objectIds, boolean fetchOnly);

  void notifyUnblocked();

  UniqueID generateTaskId(UniqueID driverId, UniqueID parentTaskId, int taskIndex);

  List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns);
}
