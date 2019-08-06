package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.generated.Common.ActorCreationTaskSpec;
import org.ray.runtime.generated.Common.ActorTaskSpec;
import org.ray.runtime.generated.Common.Language;
import org.ray.runtime.generated.Common.TaskArg;
import org.ray.runtime.generated.Common.TaskSpec;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.nativeTypes.NativeRayObject;
import org.ray.runtime.objectstore.MockObjectInterface;
import org.ray.runtime.task.FunctionArg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockTaskInterface implements TaskInterface {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockTaskInterface.class);

  private final Map<ObjectId, Set<TaskSpec>> waitingTasks = new HashMap<>();
  private final Object taskAndObjectLock = new Object();
  private final RayDevRuntime runtime;
  private final MockObjectInterface objectInterface;
  private final ExecutorService exec;
  private final Deque<MockWorker> idleWorkers = new ArrayDeque<>();
  private final Map<UniqueId, MockWorker> actorWorkers = new HashMap<>();
  private final Object workerLock = new Object();
  private final ThreadLocal<MockWorker> currentWorker = new ThreadLocal<>();

  public MockTaskInterface(RayDevRuntime runtime, MockObjectInterface objectInterface,
      int numberThreads) {
    this.runtime = runtime;
    this.objectInterface = objectInterface;
    // The thread pool that executes tasks in parallel.
    exec = Executors.newFixedThreadPool(numberThreads);
  }

  void onObjectPut(ObjectId id) {
    Set<TaskSpec> tasks;
    synchronized (taskAndObjectLock) {
      tasks = waitingTasks.remove(id);
      if (tasks != null) {
        for (TaskSpec task : tasks) {
          Set<ObjectId> unreadyObjects = getUnreadyObjects(task);
          if (unreadyObjects.isEmpty()) {
            submitTaskSpec(task);
          }
        }
      }
    }
  }

  /**
   * Get the worker of current thread. <br>
   * NOTE: Cannot be used for multi-threading in worker.
   */
  public MockWorker getCurrentWorker() {
    return currentWorker.get();
  }

  /**
   * Get a worker from the worker pool to run the given task.
   */
  private MockWorker getWorker(TaskSpec task) {
    MockWorker worker;
    synchronized (workerLock) {
      if (task.getType() == TaskType.ACTOR_TASK) {
        worker = actorWorkers.get(getActorId(task));
      } else if (task.getType() == TaskType.ACTOR_CREATION_TASK) {
        worker = new MockWorker(runtime);
        actorWorkers.put(getActorId(task), worker);
      } else if (idleWorkers.size() > 0) {
        worker = idleWorkers.pop();
      } else {
        worker = new MockWorker(runtime);
      }
    }
    currentWorker.set(worker);
    return worker;
  }

  /**
   * Return the worker to the worker pool.
   */
  private void returnWorker(MockWorker worker, TaskSpec taskSpec) {
    currentWorker.remove();
    synchronized (workerLock) {
      if (taskSpec.getType() == TaskType.NORMAL_TASK) {
        idleWorkers.push(worker);
      }
    }
  }

  private Set<ObjectId> getUnreadyObjects(TaskSpec taskSpec) {
    Set<ObjectId> unreadyObjects = new HashSet<>();
    // Check whether task arguments are ready.
    for (TaskArg arg : taskSpec.getArgsList()) {
      for (ByteString idByteString : arg.getObjectIdsList()) {
        ObjectId id = new ObjectId(idByteString.toByteArray());
        if (!objectInterface.isObjectReady(id)) {
          unreadyObjects.add(id);
        }
      }
    }
    if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      ObjectId dummyObjectId = new ObjectId(
          taskSpec.getActorTaskSpec().getPreviousActorTaskDummyObjectId().toByteArray());
      if (!objectInterface.isObjectReady(dummyObjectId)) {
        unreadyObjects.add(dummyObjectId);
      }
    }
    return unreadyObjects;
  }

  private TaskSpec.Builder getTaskSpecBuilder(TaskType taskType,
      FunctionDescriptor functionDescriptor, FunctionArg[] args) {
    return TaskSpec.newBuilder()
        .setType(taskType)
        .setLanguage(Language.JAVA)
        .setJobId(
            ByteString.copyFrom(runtime.getRayConfig().getJobId().getBytes()))
        .setTaskId(ByteString.copyFrom(TaskId.randomId().getBytes()))
        .addAllFunctionDescriptor(functionDescriptor.toList().stream().map(ByteString::copyFromUtf8)
            .collect(Collectors.toList()))
        .addAllArgs(Arrays.stream(args).map(arg -> arg.id != null ? TaskArg.newBuilder()
            .addObjectIds(ByteString.copyFrom(arg.id.getBytes())).build()
            : TaskArg.newBuilder().setData(ByteString.copyFrom(arg.data)).build())
            .collect(Collectors.toList()));
  }

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      int numReturns, CallOptions options) {
    Preconditions.checkState(numReturns == 1);
    TaskSpec taskSpec = getTaskSpecBuilder(TaskType.NORMAL_TASK, functionDescriptor, args)
        .setNumReturns(numReturns)
        .build();
    submitTaskSpec(taskSpec);
    return getReturnIds(taskSpec);
  }

  @Override
  public RayActor createActor(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      ActorCreationOptions options) {
    UniqueId actorId = UniqueId.randomId();
    TaskSpec taskSpec = getTaskSpecBuilder(TaskType.ACTOR_CREATION_TASK, functionDescriptor, args)
        .setNumReturns(1)
        .setActorCreationTaskSpec(ActorCreationTaskSpec.newBuilder()
            .setActorId(ByteString.copyFrom(actorId.toByteBuffer()))
            .build())
        .build();
    submitTaskSpec(taskSpec);
    return new MockRayActor(actorId);
  }

  @Override
  public List<ObjectId> submitActorTask(RayActor actor, FunctionDescriptor functionDescriptor,
      FunctionArg[] args, int numReturns, CallOptions options) {
    Preconditions.checkState(numReturns == 1);
    TaskSpec.Builder builder = getTaskSpecBuilder(TaskType.ACTOR_TASK, functionDescriptor, args);
    List<ObjectId> returnIds = getReturnIds(TaskType.ACTOR_TASK,
        new TaskId(builder.getTaskId().toByteArray()),
        actor.getId(), numReturns + 1);
    TaskSpec taskSpec = builder
        .setNumReturns(numReturns + 1)
        .setActorTaskSpec(
            ActorTaskSpec.newBuilder().setActorId(ByteString.copyFrom(actor.getId().getBytes()))
                .setPreviousActorTaskDummyObjectId(ByteString.copyFrom(
                    ((MockRayActor) actor)
                        .exchangePreviousActorTaskDummyObjectId(returnIds.get(returnIds.size() - 1))
                        .getBytes()))
                .build())
        .build();
    submitTaskSpec(taskSpec);
    return Collections.singletonList(returnIds.get(0));
  }

  public static UniqueId getActorId(TaskSpec taskSpec) {
    ByteString actorId = null;
    if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
      actorId = taskSpec.getActorCreationTaskSpec().getActorId();
    } else if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      actorId = taskSpec.getActorTaskSpec().getActorId();
    }
    if (actorId == null) {
      return null;
    }
    return new UniqueId(actorId.toByteArray());
  }

  private void submitTaskSpec(TaskSpec taskSpec) {
    LOGGER.debug("Submitting task: {}.", taskSpec);
    synchronized (taskAndObjectLock) {
      Set<ObjectId> unreadyObjects = getUnreadyObjects(taskSpec);
      if (unreadyObjects.isEmpty()) {
        // If all dependencies are ready, execute this task.
        exec.submit(() -> {
          MockWorker worker = getWorker(taskSpec);
          try {
            List<NativeRayObject> args = Arrays.stream(getFunctionArgs(taskSpec))
                .map(arg -> arg.id != null ? objectInterface.get(
                    Collections.singletonList(arg.id), -1).get(0)
                    : new NativeRayObject(arg.data, null)).collect(Collectors.toList());
            ((MockWorkerContext) worker.workerContext).setCurrentTask(taskSpec);
            List<NativeRayObject> returnObjects = worker
                .execute(getJavaFunctionDescriptor(taskSpec).toList(), args);
            ((MockWorkerContext) worker.workerContext).setCurrentTask(null);
            List<ObjectId> returnIds = getReturnIds(taskSpec);
            for (int i = 0; i < returnObjects.size(); i++) {
              objectInterface.put(returnObjects.get(i), returnIds.get(i));
            }
            // If the task is an actor task or an actor creation task,
            // put the dummy object in object store, so those tasks which depends on it
            // can be executed.
            if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK
                || taskSpec.getType() == TaskType.ACTOR_TASK) {
              Preconditions.checkState(returnObjects.size() + 1 == returnIds.size());
              objectInterface.put(new NativeRayObject(new byte[]{}, new byte[]{}),
                  returnIds.get(returnIds.size() - 1));
            }
          } finally {
            returnWorker(worker, taskSpec);
          }
        });
      } else {
        // If some dependencies aren't ready yet, put this task in waiting list.
        for (ObjectId id : unreadyObjects) {
          waitingTasks.computeIfAbsent(id, k -> new HashSet<>()).add(taskSpec);
        }
      }
    }
  }

  private static JavaFunctionDescriptor getJavaFunctionDescriptor(TaskSpec taskSpec) {
    List<ByteString> functionDescriptor = taskSpec.getFunctionDescriptorList();
    return new JavaFunctionDescriptor(functionDescriptor.get(0).toStringUtf8(),
        functionDescriptor.get(1).toStringUtf8(), functionDescriptor.get(2).toStringUtf8());
  }

  private static FunctionArg[] getFunctionArgs(TaskSpec taskSpec) {
    FunctionArg[] functionArgs = new FunctionArg[taskSpec.getArgsCount()];
    for (int i = 0; i < taskSpec.getArgsCount(); i++) {
      TaskArg arg = taskSpec.getArgs(i);
      if (arg.getObjectIdsCount() > 0) {
        functionArgs[i] = FunctionArg
            .passByReference(new ObjectId(arg.getObjectIds(0).toByteArray()));
      } else {
        functionArgs[i] = FunctionArg.passByValue(arg.getData().toByteArray());
      }
    }
    return functionArgs;
  }

  private static List<ObjectId> getReturnIds(TaskSpec taskSpec) {
    return getReturnIds(taskSpec.getType(), new TaskId(taskSpec.getTaskId().toByteArray()),
        getActorId(taskSpec), taskSpec.getNumReturns());
  }

  private static List<ObjectId> getReturnIds(TaskType taskType, TaskId taskId, UniqueId actorId,
      long numReturns) {
    List<ObjectId> returnIds = new ArrayList<>();
    for (int i = 0; i < numReturns; i++) {
      if (i + 1 == numReturns && taskType == TaskType.ACTOR_CREATION_TASK) {
        returnIds.add(new ObjectId(actorId.getBytes()));
      } else {
        returnIds.add(ObjectId.fromByteBuffer(
            (ByteBuffer) ByteBuffer.allocate(ObjectId.LENGTH).put(taskId.getBytes())
                .putInt(TaskId.LENGTH, i + 1).position(0)));
      }
    }
    return returnIds;
  }

}
