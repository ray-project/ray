package org.ray.spi.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.api.UniqueID;
import org.ray.core.LocalFunctionManager;
import org.ray.core.Worker;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.TaskSpec;

/**
 * A mock implementation of {@code org.ray.spi.LocalSchedulerLink}, which stores waiting tasks in a
 * Map, and cooperates with a {@code org.ray.spi.impl.MockObjectStore}.
 */
public class MockLocalScheduler implements LocalSchedulerLink {

  private final Map<UniqueID, Map<UniqueID, TaskSpec>> waitTasks_ = new ConcurrentHashMap<>();
  private final MockObjectStore store_;
  private LocalFunctionManager functions_ = null;

  public MockLocalScheduler(MockObjectStore store) {
    store_ = store;
    store.registerScheduler(this);
  }

  public void setLocalFunctionManager(LocalFunctionManager mgr) {
    functions_ = mgr;
  }

  public void onObjectPut(UniqueID id) {
    Map<UniqueID, TaskSpec> bucket = waitTasks_.get(id);
    if (bucket != null) {
      waitTasks_.remove(id);
      for (TaskSpec ts : bucket.values()) {
        submitTask(ts);
      }
    }
  }

  private UniqueID isTaskReady(TaskSpec spec) {
    for (FunctionArg arg : spec.args) {
      if (arg.ids != null) {
        for (UniqueID id : arg.ids) {
          if (!store_.isObjectReady(id)) {
            return id;
          }
        }
      }
    }
    return null;
  }

  @Override
  public void submitTask(TaskSpec task) {
    UniqueID id = isTaskReady(task);
    if (id == null) {
      Worker.execute(task, functions_);
    } else {
      Map<UniqueID, TaskSpec> bucket = waitTasks_
          .computeIfAbsent(id, id_ -> new ConcurrentHashMap<>());
      bucket.put(id, task);
    }
  }

  @Override
  public TaskSpec getTaskTodo() {
    throw new RuntimeException("invalid execution flow here");
  }

  @Override
  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {

  }

  @Override
  public void reconstructObject(UniqueID objectId) {

  }

  @Override
  public void notifyUnblocked() {

  }
}
