package org.ray.spi.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.core.LocalFunctionManager;
import org.ray.core.Worker;
import org.ray.core.impl.RayDevRuntime;
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
  private final RayDevRuntime runtime;

  public MockLocalScheduler(RayDevRuntime runtime, MockObjectStore store) {
    this.runtime = runtime;
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
      runtime.getWorker().execute(task);
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
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int timeout) {
    throw new RuntimeException("Not implemented here.");
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    return;
  }
}
