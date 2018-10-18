package org.ray.runtime.raylet;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.logger.RayLog;

/**
 * A mock implementation of RayletClient, used in single process mode.
 */
public class MockRayletClient implements RayletClient {

  private final Map<UniqueId, Map<UniqueId, TaskSpec>> waitTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private final RayDevRuntime runtime;
  private final ExecutorService exec;

  public MockRayletClient(RayDevRuntime runtime, MockObjectStore store, int nThreads) {
    this.runtime = runtime;
    this.store = store;
    store.registerScheduler(this);
    exec = Executors.newFixedThreadPool(nThreads);
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
      exec.submit(() -> {
        runtime.getWorker().execute(task);
      });
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
    return UniqueId.randomId();
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int timeoutMs) {
    List<UniqueId> ids = new ArrayList<>();
    for (RayObject<T> element : waitFor) {
      ids.add(element.getId());
    }
    boolean[] ready = new boolean[ids.size()];
    try {
      Future future = exec.submit(() -> {
        int trueCount = 0;
        while (true) {
          for (int i = 0; i < ids.size(); i++) {
            if (!ready[i] && store.isObjectReady(ids.get(i))) {
              ready[i] = true;
              ++trueCount;
            }
            if (trueCount == numReturns) {
              return;
            }
          }
        }
      });
      future.get(timeoutMs,TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      RayLog.core.error("ray wait timeout, there may not be enough RayObject ready");
    } catch (InterruptedException e) {
      RayLog.core.error("ray wait is interrupted",e);
    } catch (ExecutionException e) {
      RayLog.core.error("ray wait error",e);
    }
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();
    for (int i = 0; i < ready.length; i++) {
      if (ready[i]) {
        readyList.add(waitFor.get(i));
      } else {
        unreadyList.add(waitFor.get(i));
      }
    }
    return new WaitResult<>(readyList,unreadyList);
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    return;
  }

  public void destroy() {
    exec.shutdown();
  }
}
