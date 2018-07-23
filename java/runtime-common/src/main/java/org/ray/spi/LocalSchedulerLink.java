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

  void reconstructObject(UniqueID objectId);

  void notifyUnblocked();

  List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns);

  default void fetch(UniqueID objectId) {
    reconstructObject(objectId);
  }

  default void fetch(List<UniqueID> objectIds) {
    for (UniqueID objectId : objectIds) {
      reconstructObject(objectId);
    }
  }
}
