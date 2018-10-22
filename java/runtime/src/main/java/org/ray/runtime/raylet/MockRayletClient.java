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
import org.ray.runtime.util.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mock implementation of RayletClient, used in single process mode.
 */
public class MockRayletClient implements RayletClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockRayletClient.class);

  // tasks wait to be executed
  private final Map<UniqueId, TaskSpec> waitingTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private final RayDevRuntime runtime;
  private final ExecutorService exec;

  public MockRayletClient(RayDevRuntime runtime, MockObjectStore store, int numberThreads) {
    this.runtime = runtime;
    this.store = store;
    store.registerScheduler(this);
    // thread pool to execute tasks in parallel
    exec = Executors.newFixedThreadPool(numberThreads);
  }

  public void onObjectPut(UniqueId id) {
    TaskSpec ts = waitingTasks.get(id);
    if (ts != null) {
      waitingTasks.remove(id);
      submitTask(ts);
    }
  }

  @Override
  public void submitTask(TaskSpec task) {
    UniqueId id = getUnreadyObject(task);
    if (id == null) {
      exec.submit(() -> {
        runtime.getWorker().execute(task);
        //to simulate raylet's backend
        UniqueId[] returnIds = task.returnIds;
        store.put(returnIds[returnIds.length - 1].getBytes(),
                Serializer.encode(new Object()), null);
      });
    } else {
      waitingTasks.put(id,task);
    }
  }

  /**
   * check is every object that this task needs is ready.
   */
  private UniqueId getUnreadyObject(TaskSpec spec) {
    //check whether the arguments which this task needs is ready
    for (FunctionArg arg : spec.args) {
      if (arg.id != null) {
        if (!store.isObjectReady(arg.id)) {
          // if this objectId doesn't exist in store ,then return this objectId
          return arg.id;
        }
      }
    }
    //check whether the dependencies which this task needs is ready
    for (UniqueId id : spec.getExecutionDependencies()) {
      if (!store.isObjectReady(id)) {
        return id;
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
        while (trueCount < numReturns) {
          for (int i = 0; i < ids.size(); i++) {
            if (!ready[i] && store.isObjectReady(ids.get(i))) {
              ready[i] = true;
              ++trueCount;
            }
          }
        }
      });
      future.get(timeoutMs,TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOGGER.error("ray wait timeout, there may not be enough RayObject ready");
    } catch (InterruptedException e) {
      LOGGER.error("ray wait is interrupted", e);
    } catch (ExecutionException e) {
      LOGGER.error("ray wait error", e);
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
