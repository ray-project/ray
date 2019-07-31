package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
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

  private final ConcurrentMap<ObjectId, Set<TaskSpec>> waitingTasks = new ConcurrentHashMap<>();
  private final RayDevRuntime runtime;
  private final MockObjectInterface objectInterface;
  private final ExecutorService exec;
  private final ConcurrentLinkedDeque<MockWorker> idleWorkers = new ConcurrentLinkedDeque<>();
  private final ConcurrentMap<UniqueId, MockWorker> actorWorkers = new ConcurrentHashMap<>();
  private final ThreadLocal<MockWorker> currentWorker = new ThreadLocal<>();
  private final ConcurrentMap<UniqueId, RayActor> actorHandles = new ConcurrentHashMap<>();

  public MockTaskInterface(RayDevRuntime runtime, MockObjectInterface objectInterface,
      int numberThreads) {
    this.runtime = runtime;
    this.objectInterface = objectInterface;
    objectInterface.addObjectPutCallback(this::onObjectPut);
    // The thread pool that executes tasks in parallel.
    exec = Executors.newFixedThreadPool(numberThreads);
  }

  synchronized void onObjectPut(ObjectId id) {
    Set<TaskSpec> tasks = waitingTasks.get(id);
    if (tasks != null) {
      waitingTasks.remove(id);
      for (TaskSpec task : tasks) {
        switch (task.getType()) {
          case NORMAL_TASK:
            submitTask(getJavaFunctionDescriptor(task), getFunctionArgs(task),
                (int) task.getNumReturns(),
                null);
            break;
          case ACTOR_CREATION_TASK:
            createActor(getJavaFunctionDescriptor(task), getFunctionArgs(task), null);
            break;
          case ACTOR_TASK:
            submitActorTask(
                actorHandles.get(getActorId(task)), getJavaFunctionDescriptor(task),
                getFunctionArgs(task),
                (int) task.getNumReturns(), null);
            break;
        }
      }
    }
  }

  private MockWorker getCurrentWorker() {
    return currentWorker.get();
  }

  /**
   * Get a worker from the worker pool to run the given task.
   */
  private synchronized MockWorker getWorker(TaskSpec task) {
    MockWorker worker;
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
    currentWorker.set(worker);
    return worker;
  }

  /**
   * Return the worker to the worker pool.
   */
  private void returnWorker(MockWorker worker) {
    currentWorker.remove();
    idleWorkers.push(worker);
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
            ByteString.copyFrom(getCurrentWorker().getWorkerContext().getCurrentJobId().getBytes()))
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
            .setActorId(ByteString.copyFrom(actorId.toByteBuffer())).build())
        .build();
    submitTaskSpec(taskSpec);
    MockRayActor actor = new MockRayActor(actorId);
    actorHandles.put(actorId, actor);
    return actor;
  }

  @Override
  public List<ObjectId> submitActorTask(RayActor actor, FunctionDescriptor functionDescriptor,
      FunctionArg[] args, int numReturns, CallOptions options) {
    TaskSpec taskSpec = getTaskSpecBuilder(TaskType.ACTOR_TASK, functionDescriptor, args)
        .setNumReturns(numReturns + 1)
        .setActorTaskSpec(
            ActorTaskSpec.newBuilder().setActorId(ByteString.copyFrom(actor.getId().getBytes()))
                .build())
        .build();
    submitTaskSpec(taskSpec);
    return getReturnIds(taskSpec);
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
          // If the task is an actor task or an actor creation task,
          // put the dummy object in object store, so those tasks which depends on it
          // can be executed.
          UniqueId actorId = getActorId(taskSpec);
          List<ObjectId> returnIds = getReturnIds(taskSpec);
          for (int i = 0; i < returnObjects.size(); i++) {
            objectInterface.put(returnObjects.get(i), returnIds.get(i));
          }
          if (actorId != null) {
            Preconditions.checkState(returnObjects.size() + 1 == returnIds.size());
            objectInterface.put(new NativeRayObject(new byte[]{}, new byte[]{}),
                returnIds.get(returnIds.size() - 1));
          }
        } finally {
          returnWorker(worker);
        }
      });
    } else {
      // If some dependencies aren't ready yet, put this task in waiting list.
      for (ObjectId id : unreadyObjects) {
        waitingTasks.computeIfAbsent(id, k -> new HashSet<>()).add(taskSpec);
      }
    }
  }

  private static JavaFunctionDescriptor getJavaFunctionDescriptor(TaskSpec taskSpec) {
    List<ByteString> functionDescriptor = taskSpec.getFunctionDescriptorList();
    return new JavaFunctionDescriptor(functionDescriptor.get(0).toString(),
        functionDescriptor.get(1).toString(), functionDescriptor.get(2).toString());
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
    List<ObjectId> returnIds = new ArrayList<>();
    long numReturns = taskSpec.getNumReturns();
    UniqueId actorId = getActorId(taskSpec);
    if (actorId != null) {
      numReturns--;
    }

    for (int i = 0; i < numReturns; i++) {
      returnIds.add(ObjectId.fromByteBuffer(
          ByteBuffer.allocate(ObjectId.LENGTH).put(taskSpec.getTaskId().toByteArray())
              .putInt(i + 1)));
    }
    if (actorId != null) {
      returnIds.add(new ObjectId(actorId.getBytes()));
    }
    return returnIds;
  }

  static class MockRayActor<T> implements RayActor<T> {

    private final UniqueId actorId;

    public MockRayActor(UniqueId actorId) {
      this.actorId = actorId;
    }

    @Override
    public UniqueId getId() {
      return actorId;
    }

    @Override
    public UniqueId getHandleId() {
      return UniqueId.NIL;
    }
  }
}
