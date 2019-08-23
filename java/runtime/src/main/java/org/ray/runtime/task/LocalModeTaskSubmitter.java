package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.actor.LocalModeRayActor;
import org.ray.runtime.context.LocalModeWorkerContext;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.generated.Common.ActorCreationTaskSpec;
import org.ray.runtime.generated.Common.ActorTaskSpec;
import org.ray.runtime.generated.Common.Language;
import org.ray.runtime.generated.Common.TaskArg;
import org.ray.runtime.generated.Common.TaskSpec;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.object.NativeRayObject;
import org.ray.runtime.object.LocalModeObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task submitter for local mode.
 */
public class LocalModeTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalModeTaskSubmitter.class);

  private final Map<ObjectId, Set<TaskSpec>> waitingTasks = new HashMap<>();
  private final Object taskAndObjectLock = new Object();
  private final RayDevRuntime runtime;
  private final LocalModeObjectStore objectStore;
  private final ExecutorService exec;
  private final Deque<TaskExecutor> idleTaskExecutors = new ArrayDeque<>();
  private final Map<ActorId, TaskExecutor> actorTaskExecutors = new HashMap<>();
  private final Object taskExecutorLock = new Object();
  private final ThreadLocal<TaskExecutor> currentTaskExecutor = new ThreadLocal<>();

  public LocalModeTaskSubmitter(RayDevRuntime runtime, LocalModeObjectStore objectStore,
      int numberThreads) {
    this.runtime = runtime;
    this.objectStore = objectStore;
    // The thread pool that executes tasks in parallel.
    exec = Executors.newFixedThreadPool(numberThreads);
  }

  public void onObjectPut(ObjectId id) {
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
   * Get the worker of current thread. <br> NOTE: Cannot be used for multi-threading in worker.
   */
  public TaskExecutor getCurrentTaskExecutor() {
    return currentTaskExecutor.get();
  }

  /**
   * Get a worker from the worker pool to run the given task.
   */
  private TaskExecutor getTaskExecutor(TaskSpec task) {
    TaskExecutor taskExecutor;
    synchronized (taskExecutorLock) {
      if (task.getType() == TaskType.ACTOR_TASK) {
        taskExecutor = actorTaskExecutors.get(getActorId(task));
      } else if (task.getType() == TaskType.ACTOR_CREATION_TASK) {
        taskExecutor = new TaskExecutor(runtime);
        actorTaskExecutors.put(getActorId(task), taskExecutor);
      } else if (idleTaskExecutors.size() > 0) {
        taskExecutor = idleTaskExecutors.pop();
      } else {
        taskExecutor = new TaskExecutor(runtime);
      }
    }
    currentTaskExecutor.set(taskExecutor);
    return taskExecutor;
  }

  /**
   * Return the worker to the worker pool.
   */
  private void returnTaskExecutor(TaskExecutor worker, TaskSpec taskSpec) {
    currentTaskExecutor.remove();
    synchronized (taskExecutorLock) {
      if (taskSpec.getType() == TaskType.NORMAL_TASK) {
        idleTaskExecutors.push(worker);
      }
    }
  }

  private Set<ObjectId> getUnreadyObjects(TaskSpec taskSpec) {
    Set<ObjectId> unreadyObjects = new HashSet<>();
    // Check whether task arguments are ready.
    for (TaskArg arg : taskSpec.getArgsList()) {
      for (ByteString idByteString : arg.getObjectIdsList()) {
        ObjectId id = new ObjectId(idByteString.toByteArray());
        if (!objectStore.isObjectReady(id)) {
          unreadyObjects.add(id);
        }
      }
    }
    if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      ObjectId dummyObjectId = new ObjectId(
          taskSpec.getActorTaskSpec().getPreviousActorTaskDummyObjectId().toByteArray());
      if (!objectStore.isObjectReady(dummyObjectId)) {
        unreadyObjects.add(dummyObjectId);
      }
    }
    return unreadyObjects;
  }

  private TaskSpec.Builder getTaskSpecBuilder(TaskType taskType,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args) {
    byte[] taskIdBytes = new byte[TaskId.LENGTH];
    new Random().nextBytes(taskIdBytes);
    return TaskSpec.newBuilder()
        .setType(taskType)
        .setLanguage(Language.JAVA)
        .setJobId(
            ByteString.copyFrom(runtime.getRayConfig().getJobId().getBytes()))
        .setTaskId(ByteString.copyFrom(taskIdBytes))
        .addAllFunctionDescriptor(functionDescriptor.toList().stream().map(ByteString::copyFromUtf8)
            .collect(Collectors.toList()))
        .addAllArgs(args.stream().map(arg -> arg.id != null ? TaskArg.newBuilder()
            .addObjectIds(ByteString.copyFrom(arg.id.getBytes())).build()
            : TaskArg.newBuilder().setData(ByteString.copyFrom(arg.data)).build())
            .collect(Collectors.toList()));
  }

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      int numReturns, CallOptions options) {
    Preconditions.checkState(numReturns <= 1);
    TaskSpec taskSpec = getTaskSpecBuilder(TaskType.NORMAL_TASK, functionDescriptor, args)
        .setNumReturns(numReturns)
        .build();
    submitTaskSpec(taskSpec);
    return getReturnIds(taskSpec);
  }

  @Override
  public RayActor createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      ActorCreationOptions options) {
    ActorId actorId = ActorId.fromRandom();
    TaskSpec taskSpec = getTaskSpecBuilder(TaskType.ACTOR_CREATION_TASK, functionDescriptor, args)
        .setNumReturns(1)
        .setActorCreationTaskSpec(ActorCreationTaskSpec.newBuilder()
            .setActorId(ByteString.copyFrom(actorId.toByteBuffer()))
            .build())
        .build();
    submitTaskSpec(taskSpec);
    return new LocalModeRayActor(actorId, getReturnIds(taskSpec).get(0));
  }

  @Override
  public List<ObjectId> submitActorTask(RayActor actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options) {
    Preconditions.checkState(numReturns <= 1);
    TaskSpec.Builder builder = getTaskSpecBuilder(TaskType.ACTOR_TASK, functionDescriptor, args);
    List<ObjectId> returnIds = getReturnIds(
        TaskId.fromBytes(builder.getTaskId().toByteArray()), numReturns + 1);
    TaskSpec taskSpec = builder
        .setNumReturns(numReturns + 1)
        .setActorTaskSpec(
            ActorTaskSpec.newBuilder().setActorId(ByteString.copyFrom(actor.getId().getBytes()))
                .setPreviousActorTaskDummyObjectId(ByteString.copyFrom(
                    ((LocalModeRayActor) actor)
                        .exchangePreviousActorTaskDummyObjectId(returnIds.get(returnIds.size() - 1))
                        .getBytes()))
                .build())
        .build();
    submitTaskSpec(taskSpec);
    if (numReturns  == 0) {
      return ImmutableList.of();
    } else {
      return ImmutableList.of(returnIds.get(0));
    }
  }

  public static ActorId getActorId(TaskSpec taskSpec) {
    ByteString actorId = null;
    if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
      actorId = taskSpec.getActorCreationTaskSpec().getActorId();
    } else if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      actorId = taskSpec.getActorTaskSpec().getActorId();
    }
    if (actorId == null) {
      return null;
    }
    return ActorId.fromBytes(actorId.toByteArray());
  }

  private void submitTaskSpec(TaskSpec taskSpec) {
    LOGGER.debug("Submitting task: {}.", taskSpec);
    synchronized (taskAndObjectLock) {
      Set<ObjectId> unreadyObjects = getUnreadyObjects(taskSpec);
      if (unreadyObjects.isEmpty()) {
        // If all dependencies are ready, execute this task.
        exec.submit(() -> {
          TaskExecutor taskExecutor = getTaskExecutor(taskSpec);
          try {
            List<NativeRayObject> args = getFunctionArgs(taskSpec).stream()
                .map(arg -> arg.id != null ?
                    objectStore.getRaw(Collections.singletonList(arg.id), -1).get(0)
                    : new NativeRayObject(arg.data, null))
                .collect(Collectors.toList());
            ((LocalModeWorkerContext) runtime.getWorkerContext()).setCurrentTask(taskSpec);
            List<NativeRayObject> returnObjects = taskExecutor
                .execute(getJavaFunctionDescriptor(taskSpec).toList(), args);
            ((LocalModeWorkerContext) runtime.getWorkerContext()).setCurrentTask(null);
            List<ObjectId> returnIds = getReturnIds(taskSpec);
            for (int i = 0; i < returnIds.size(); i++) {
              NativeRayObject putObject;
              if (i >= returnObjects.size()) {
                // If the task is an actor task or an actor creation task,
                // put the dummy object in object store, so those tasks which depends on it
                // can be executed.
                putObject = new NativeRayObject(new byte[]{}, new byte[]{});
              } else {
                putObject = returnObjects.get(i);
              }
              objectStore.putRaw(putObject, returnIds.get(i));
            }
          } finally {
            returnTaskExecutor(taskExecutor, taskSpec);
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

  private static List<FunctionArg> getFunctionArgs(TaskSpec taskSpec) {
    List<FunctionArg> functionArgs = new ArrayList<>();
    for (int i = 0; i < taskSpec.getArgsCount(); i++) {
      TaskArg arg = taskSpec.getArgs(i);
      if (arg.getObjectIdsCount() > 0) {
        functionArgs.add(FunctionArg
            .passByReference(new ObjectId(arg.getObjectIds(0).toByteArray())));
      } else {
        functionArgs.add(FunctionArg.passByValue(arg.getData().toByteArray()));
      }
    }
    return functionArgs;
  }

  private static List<ObjectId> getReturnIds(TaskSpec taskSpec) {
    return getReturnIds(TaskId.fromBytes(taskSpec.getTaskId().toByteArray()),
        taskSpec.getNumReturns());
  }

  private static List<ObjectId> getReturnIds(TaskId taskId, long numReturns) {
    List<ObjectId> returnIds = new ArrayList<>();
    for (int i = 0; i < numReturns; i++) {
      returnIds.add(ObjectId.fromByteBuffer(
          (ByteBuffer) ByteBuffer.allocate(ObjectId.LENGTH).put(taskId.getBytes())
              .putInt(TaskId.LENGTH, i + 1).position(0)));
    }
    return returnIds;
  }

}
