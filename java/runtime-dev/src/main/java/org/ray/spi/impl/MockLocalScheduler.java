package org.ray.spi.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.api.id.UniqueId;
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

  private final Map<UniqueId, Map<UniqueId, TaskSpec>> waitTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private LocalFunctionManager functions = null;

  public MockLocalScheduler(MockObjectStore store) {
    this.store = store;
    store.registerScheduler(this);
  }

  public void setLocalFunctionManager(LocalFunctionManager mgr) {
    functions = mgr;
  }

  public void onObjectPut(UniqueId id) {
    Map<UniqueId, TaskSpec> bucket = waitTasks.get(id);
    if (bucket != null) {
      waitTasks.remove(id);
      for (TaskSpec ts : bucket.values()) {
        submitTask(ts);
      }
    }
  }

  @Override
  public void submitTask(TaskSpec task) {
    UniqueId id = isTaskReady(task);
    if (id == null) {
      Worker.execute(task, functions);
    } else {
      Map<UniqueId, TaskSpec> bucket = waitTasks
          .computeIfAbsent(id, id_ -> new ConcurrentHashMap<>());
      bucket.put(id, task);
    }
  }

  private UniqueId isTaskReady(TaskSpec spec) {
    for (FunctionArg arg : spec.args) {
      if (arg.id != null) {
        if (!store.isObjectReady(arg.id)) {
          return arg.id;
        }
      }
    }
    return null;
  }

  @Override
  public TaskSpec getTask() {
    throw new RuntimeException("invalid execution flow here");
  }

  @Override
  public void markTaskPutDependency(UniqueId taskId, UniqueId objectId) {

  }

  @Override
  public void reconstructObject(UniqueId objectId, boolean fetchOnly) {

  }

  @Override
  public void reconstructObjects(List<UniqueId> objectIds, boolean fetchOnly) {

  }

  @Override
  public void notifyUnblocked() {

  }

  @Override
  public UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex) {
    throw new RuntimeException("Not implemented here.");
  }

  @Override
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    return store.wait(objectIds, timeoutMs, numReturns);
  }
}
