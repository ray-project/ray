package org.ray.runtime.raylet;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;

/**
 * A mock implementation of RayletClient, used in single process mode.
 */
public class MockRayletClient implements RayletClient {

  private final Map<UniqueId, Map<UniqueId, TaskSpec>> waitTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private final RayDevRuntime runtime;

  public MockRayletClient(RayDevRuntime runtime, MockObjectStore store) {
    this.runtime = runtime;
    this.store = store;
    store.registerScheduler(this);
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
  public void fetchOrReconstruct(List<UniqueId> objectIds, boolean fetchOnly,
      UniqueId currentTaskId) {

  }

  @Override
  public void notifyUnblocked(UniqueId currentTaskId) {

  }

  @Override
  public UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex) {
    return UniqueId.randomId();
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int
      timeoutMs, UniqueId currentTaskId) {
    return new WaitResult<T>(
        waitFor,
        ImmutableList.of()
    );
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    return;
  }
}
