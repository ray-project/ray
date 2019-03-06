package org.ray.runtime.raylet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.NotImplementedException;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.Worker;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mock implementation of RayletClient, used in single process mode.
 */
public class MockRayletClient implements RayletClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockRayletClient.class);

  private final Map<UniqueId, Set<TaskSpec>> waitingTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private final RayDevRuntime runtime;
  private final ExecutorService exec;
  private final Deque<Worker> idleWorkers;
  private final Map<UniqueId, Worker> actorWorkers;

  public MockRayletClient(RayDevRuntime runtime, int numberThreads) {
    this.runtime = runtime;
    this.store = runtime.getObjectStore();
    store.addObjectPutCallback(this::onObjectPut);
    // The thread pool that executes tasks in parallel.
    exec = Executors.newFixedThreadPool(numberThreads);
    idleWorkers = new LinkedList<>();
    actorWorkers = new HashMap<>();
  }

  public synchronized void onObjectPut(UniqueId id) {
    Set<TaskSpec> tasks = waitingTasks.get(id);
    if (tasks != null) {
      waitingTasks.remove(id);
      for (TaskSpec taskSpec : tasks) {
        submitTask(taskSpec);
      }
    }
  }

  /**
   * Get a worker from the worker pool to run the given task.
   */
  private Worker getWorker(TaskSpec task) {
    if (task.isActorTask()) {
      return actorWorkers.get(task.actorId);
    }
    Worker worker;
    if (idleWorkers.size() > 0) {
      worker = idleWorkers.pop();
    } else {
      worker = new Worker(runtime);
    }
    if (task.isActorCreationTask()) {
      actorWorkers.put(task.actorCreationId, worker);
    }
    return worker;
  }

  /**
   * Return the worker to the worker pool.
   */
  private void returnWorker(Worker worker) {
    idleWorkers.push(worker);
  }

  @Override
  public synchronized void submitTask(TaskSpec task) {
    LOGGER.debug("Submitting task: {}.", task);
    Set<UniqueId> unreadyObjects = getUnreadyObjects(task);
    if (unreadyObjects.isEmpty()) {
      // If all dependencies are ready, execute this task.
      exec.submit(() -> {
        Worker worker = getWorker(task);
        try {
          worker.execute(task);
          // If the task is an actor task or an actor creation task,
          // put the dummy object in object store, so those tasks which depends on it
          // can be executed.
          if (task.isActorCreationTask() || task.isActorTask()) {
            UniqueId[] returnIds = task.returnIds;
            store.put(returnIds[returnIds.length - 1].getBytes(),
                    new byte[]{}, new byte[]{});
          }
        } finally {
          if (!task.isActorCreationTask() && !task.isActorTask()) {
            returnWorker(worker);
          }
        }
      });
    } else {
      // If some dependencies aren't ready yet, put this task in waiting list.
      for (UniqueId id : unreadyObjects) {
        waitingTasks.computeIfAbsent(id, k -> new HashSet<>()).add(task);
      }
    }
  }

  private Set<UniqueId> getUnreadyObjects(TaskSpec spec) {
    Set<UniqueId> unreadyObjects = new HashSet<>();
    // Check whether task arguments are ready.
    for (FunctionArg arg : spec.args) {
      if (arg.id != null) {
        if (!store.isObjectReady(arg.id)) {
          unreadyObjects.add(arg.id);
        }
      }
    }
    // Check whether task dependencies are ready.
    for (UniqueId id : spec.getExecutionDependencies()) {
      if (!store.isObjectReady(id)) {
        unreadyObjects.add(id);
      }
    }
    return unreadyObjects;
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
    if (waitFor == null || waitFor.isEmpty()) {
      return new WaitResult<>(ImmutableList.of(), ImmutableList.of());
    }

    byte[][] ids = new byte[waitFor.size()][];
    for (int i = 0; i < waitFor.size(); i++) {
      ids[i] = waitFor.get(i).getId().getBytes();
    }
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();
    List<byte[]> result = store.get(ids, timeoutMs, false);
    for (int i = 0; i < waitFor.size(); i++) {
      if (result.get(i) != null) {
        readyList.add(waitFor.get(i));
      } else {
        unreadyList.add(waitFor.get(i));
      }
    }
    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    for (UniqueId id : objectIds) {
      store.free(id);
    }
  }


  @Override
  public UniqueId prepareCheckpoint(UniqueId actorId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void notifyActorResumedFromCheckpoint(UniqueId actorId, UniqueId checkpointId) {
    throw new NotImplementedException("Not implemented.");
  }

  @Override
  public void destroy() {
    exec.shutdown();
  }
}
