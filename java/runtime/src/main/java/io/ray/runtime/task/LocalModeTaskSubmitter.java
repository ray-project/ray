package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.actor.LocalModeActorHandle;
import io.ray.runtime.context.LocalModeWorkerContext;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.generated.Common;
import io.ray.runtime.generated.Common.ActorCreationTaskSpec;
import io.ray.runtime.generated.Common.ActorTaskSpec;
import io.ray.runtime.generated.Common.Address;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.generated.Common.ObjectReference;
import io.ray.runtime.generated.Common.TaskArg;
import io.ray.runtime.generated.Common.TaskSpec;
import io.ray.runtime.generated.Common.TaskType;
import io.ray.runtime.object.LocalModeObjectStore;
import io.ray.runtime.object.NativeRayObject;
import io.ray.runtime.placementgroup.PlacementGroupImpl;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task submitter for local mode. */
public class LocalModeTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalModeTaskSubmitter.class);

  private final Map<ObjectId, Set<TaskSpec>> waitingTasks = new HashMap<>();
  private final Object taskAndObjectLock = new Object();
  private final RayRuntimeInternal runtime;
  private final TaskExecutor taskExecutor;
  private final LocalModeObjectStore objectStore;

  /// The thread pool to execute actor tasks.
  private final Map<ActorId, ExecutorService> actorTaskExecutorServices;

  private final Map<ActorId, Integer> actorMaxConcurrency = new ConcurrentHashMap<>();

  /// The thread pool to execute normal tasks.
  private final ExecutorService normalTaskExecutorService;

  private final Map<ActorId, LocalModeActorHandle> actorHandles = new ConcurrentHashMap<>();

  private final Map<String, ActorHandle> namedActors = new ConcurrentHashMap<>();

  private final Map<ActorId, TaskExecutor.ActorContext> actorContexts = new ConcurrentHashMap<>();

  private final Map<PlacementGroupId, PlacementGroup> placementGroups = new ConcurrentHashMap<>();

  public LocalModeTaskSubmitter(
      RayRuntimeInternal runtime, TaskExecutor taskExecutor, LocalModeObjectStore objectStore) {
    this.runtime = runtime;
    this.taskExecutor = taskExecutor;
    this.objectStore = objectStore;
    // The thread pool that executes normal tasks in parallel.
    normalTaskExecutorService = Executors.newCachedThreadPool();
    // The thread pool that executes actor tasks in parallel.
    actorTaskExecutorServices = new HashMap<>();
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

  private Set<ObjectId> getUnreadyObjects(TaskSpec taskSpec) {
    Set<ObjectId> unreadyObjects = new HashSet<>();
    // Check whether task arguments are ready.
    for (TaskArg arg : taskSpec.getArgsList()) {
      ByteString idByteString = arg.getObjectRef().getObjectId();
      if (idByteString != ByteString.EMPTY) {
        ObjectId id = new ObjectId(idByteString.toByteArray());
        if (!objectStore.isObjectReady(id)) {
          unreadyObjects.add(id);
        }
      }
    }
    if (taskSpec.getType() == TaskType.ACTOR_TASK  && !isConcurrentActor(taskSpec)) {
      ObjectId dummyObjectId =
          new ObjectId(
              taskSpec.getActorTaskSpec().getPreviousActorTaskDummyObjectId().toByteArray());
      if (!objectStore.isObjectReady(dummyObjectId)) {
        unreadyObjects.add(dummyObjectId);
      }
    }
    return unreadyObjects;
  }

  private TaskSpec.Builder getTaskSpecBuilder(
      TaskType taskType, FunctionDescriptor functionDescriptor, List<FunctionArg> args) {
    byte[] taskIdBytes = new byte[TaskId.LENGTH];
    new Random().nextBytes(taskIdBytes);
    List<String> functionDescriptorList = functionDescriptor.toList();
    Preconditions.checkState(functionDescriptorList.size() >= 3);
    return TaskSpec.newBuilder()
        .setType(taskType)
        .setLanguage(Language.JAVA)
        .setJobId(ByteString.copyFrom(runtime.getRayConfig().getJobId().getBytes()))
        .setTaskId(ByteString.copyFrom(taskIdBytes))
        .setFunctionDescriptor(
            Common.FunctionDescriptor.newBuilder()
                .setJavaFunctionDescriptor(
                    Common.JavaFunctionDescriptor.newBuilder()
                        .setClassName(functionDescriptorList.get(0))
                        .setFunctionName(functionDescriptorList.get(1))
                        .setSignature(functionDescriptorList.get(2))))
        .addAllArgs(
            args.stream()
                .map(
                    arg ->
                        arg.id != null
                            ? TaskArg.newBuilder()
                                .setObjectRef(
                                    ObjectReference.newBuilder()
                                        .setObjectId(ByteString.copyFrom(arg.id.getBytes())))
                                .build()
                            : TaskArg.newBuilder()
                                .setData(ByteString.copyFrom(arg.value.data))
                                .setMetadata(
                                    arg.value.metadata != null
                                        ? ByteString.copyFrom(arg.value.metadata)
                                        : ByteString.EMPTY)
                                .build())
                .collect(Collectors.toList()));
  }

  @Override
  public List<ObjectId> submitTask(
      FunctionDescriptor functionDescriptor,
      List<FunctionArg> args,
      int numReturns,
      CallOptions options) {
    Preconditions.checkState(numReturns <= 1);
    TaskSpec taskSpec =
        getTaskSpecBuilder(TaskType.NORMAL_TASK, functionDescriptor, args)
            .setNumReturns(numReturns)
            .build();
    submitTaskSpec(taskSpec);
    return getReturnIds(taskSpec);
  }

  @Override
  public BaseActorHandle createActor(
      FunctionDescriptor functionDescriptor, List<FunctionArg> args, ActorCreationOptions options)
      throws IllegalArgumentException {
    if (options != null) {
      if (options.group != null) {
        PlacementGroupImpl group = (PlacementGroupImpl) options.group;
        Preconditions.checkArgument(
            options.bundleIndex >= 0 && options.bundleIndex < group.getBundles().size(),
            String.format("Bundle index %s is invalid", options.bundleIndex));
      }
    }

    ActorId actorId = ActorId.fromRandom();
    TaskSpec taskSpec =
        getTaskSpecBuilder(TaskType.ACTOR_CREATION_TASK, functionDescriptor, args)
            .setNumReturns(1)
            .setActorCreationTaskSpec(
                ActorCreationTaskSpec.newBuilder()
                  .setActorId(ByteString.copyFrom(actorId.toByteBuffer()))
                  .setMaxConcurrency(options.maxConcurrency)
                  .build())
            .build();
    submitTaskSpec(taskSpec);
    final LocalModeActorHandle actorHandle =
        new LocalModeActorHandle(actorId, getReturnIds(taskSpec).get(0));
    actorHandles.put(actorId, actorHandle.copy());
    if (StringUtils.isNotBlank(options.name)) {
      String fullName =
          options.global
              ? options.name
              : String.format("%s-%s", Ray.getRuntimeContext().getCurrentJobId(), options.name);
      Preconditions.checkArgument(
          !namedActors.containsKey(fullName), String.format("Actor of name %s exists", fullName));
      namedActors.put(fullName, actorHandle);
    }
    return actorHandle;
  }

  @Override
  public List<ObjectId> submitActorTask(
      BaseActorHandle actor,
      FunctionDescriptor functionDescriptor,
      List<FunctionArg> args,
      int numReturns,
      CallOptions options) {
    Preconditions.checkState(numReturns <= 1);
    TaskSpec.Builder builder = getTaskSpecBuilder(TaskType.ACTOR_TASK, functionDescriptor, args);
    List<ObjectId> returnIds =
        getReturnIds(TaskId.fromBytes(builder.getTaskId().toByteArray()), numReturns + 1);
    TaskSpec taskSpec =
        builder
            .setNumReturns(numReturns + 1)
            .setActorTaskSpec(
                ActorTaskSpec.newBuilder()
                    .setActorId(ByteString.copyFrom(actor.getId().getBytes()))
                    .setPreviousActorTaskDummyObjectId(
                        ByteString.copyFrom(
                            ((LocalModeActorHandle) actor)
                                .exchangePreviousActorTaskDummyObjectId(
                                    returnIds.get(returnIds.size() - 1))
                                .getBytes()))
                    .build())
            .build();
    submitTaskSpec(taskSpec);
    if (numReturns == 0) {
      return ImmutableList.of();
    } else {
      return ImmutableList.of(returnIds.get(0));
    }
  }

  @Override
  public PlacementGroup createPlacementGroup(PlacementGroupCreationOptions creationOptions) {
    PlacementGroupImpl placementGroup =
        new PlacementGroupImpl.Builder()
            .setId(PlacementGroupId.fromRandom())
            .setName(creationOptions.name)
            .setBundles(creationOptions.bundles)
            .setStrategy(creationOptions.strategy)
            .build();
    placementGroups.put(placementGroup.getId(), placementGroup);
    return placementGroup;
  }

  @Override
  public void removePlacementGroup(PlacementGroupId id) {
    placementGroups.remove(id);
  }

  @Override
  public boolean waitPlacementGroupReady(PlacementGroupId id, int timeoutMs) {
    return true;
  }

  @Override
  public BaseActorHandle getActor(ActorId actorId) {
    return actorHandles.get(actorId).copy();
  }

  public Optional<BaseActorHandle> getActor(String name, boolean global) {
    String fullName =
        global ? name : String.format("%s-%s", Ray.getRuntimeContext().getCurrentJobId(), name);
    ActorHandle actorHandle = namedActors.get(fullName);
    if (null == actorHandle) {
      return Optional.empty();
    }
    return Optional.of(actorHandle);
  }

  public void shutdown() {
    // Shutdown actor task executor service.
    synchronized (actorTaskExecutorServices) {
      for (Map.Entry<ActorId, ExecutorService> item : actorTaskExecutorServices.entrySet()) {
        item.getValue().shutdown();
      }
    }
    // Shutdown normal task executor service.
    normalTaskExecutorService.shutdown();
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

      final Runnable runnable =
          () -> {
            try {
              executeTask(taskSpec);
            } catch (Exception ex) {
              LOGGER.error("Unexpected exception when executing a task.", ex);
              System.exit(-1);
            }
          };

      if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
        actorMaxConcurrency.put(
          getActorId(taskSpec), taskSpec.getActorCreationTaskSpec().getMaxConcurrency());
      }

      if (unreadyObjects.isEmpty()) {
        // If all dependencies are ready, execute this task.
        ExecutorService executorService;
        if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
          final int maxConcurrency = taskSpec.getActorCreationTaskSpec().getMaxConcurrency();
          Preconditions.checkState(maxConcurrency >= 1);
          executorService = Executors.newFixedThreadPool(maxConcurrency);
          synchronized (actorTaskExecutorServices) {
            actorTaskExecutorServices.put(getActorId(taskSpec), executorService);
          }
        } else if (taskSpec.getType() == TaskType.ACTOR_TASK) {
          synchronized (actorTaskExecutorServices) {
            executorService = actorTaskExecutorServices.get(getActorId(taskSpec));
          }
        } else {
          // Normal task.
          executorService = normalTaskExecutorService;
        }
        try {
          executorService.submit(runnable);
        } catch (RejectedExecutionException e) {
          if (executorService.isShutdown()) {
            LOGGER.warn(
                "Ignore task submission due to the ExecutorService is shutdown. Task: {}",
                taskSpec);
          }
        }
      } else {
        // If some dependencies aren't ready yet, put this task in waiting list.
        for (ObjectId id : unreadyObjects) {
          waitingTasks.computeIfAbsent(id, k -> new HashSet<>()).add(taskSpec);
        }
      }
    }
  }

  private void executeTask(TaskSpec taskSpec) {
    TaskExecutor.ActorContext actorContext = null;
    UniqueId workerId;
    if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      actorContext = actorContexts.get(getActorId(taskSpec));
      Preconditions.checkNotNull(actorContext);
      workerId = ((LocalModeTaskExecutor.LocalActorContext) actorContext).getWorkerId();
    } else {
      // Actor creation task and normal task will use a new random worker id.
      workerId = UniqueId.randomId();
    }
    taskExecutor.setActorContext(workerId, actorContext);
    List<NativeRayObject> args =
        getFunctionArgs(taskSpec).stream()
            .map(
                arg ->
                    arg.id != null
                        ? objectStore.getRaw(Collections.singletonList(arg.id), -1).get(0)
                        : arg.value)
            .collect(Collectors.toList());
    runtime.setIsContextSet(true);
    ((LocalModeWorkerContext) runtime.getWorkerContext()).setCurrentTask(taskSpec);

    ((LocalModeWorkerContext) runtime.getWorkerContext()).setCurrentWorkerId(workerId);
    List<String> rayFunctionInfo = getJavaFunctionDescriptor(taskSpec).toList();
    taskExecutor.checkByteBufferArguments(rayFunctionInfo);
    List<NativeRayObject> returnObjects = taskExecutor.execute(rayFunctionInfo, args);
    if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
      // Update actor context map ASAP in case objectStore.putRaw triggered the next actor task
      // on this actor.
      actorContexts.put(getActorId(taskSpec), taskExecutor.getActorContext());
    }
    // Set this flag to true is necessary because at the end of `taskExecutor.execute()`,
    // this flag will be set to false. And `runtime.getWorkerContext()` requires it to be
    // true.
    runtime.setIsContextSet(true);
    ((LocalModeWorkerContext) runtime.getWorkerContext()).setCurrentTask(null);
    runtime.setIsContextSet(false);
    List<ObjectId> returnIds = getReturnIds(taskSpec);
    for (int i = 0; i < returnIds.size(); i++) {
      NativeRayObject putObject;
      if (i >= returnObjects.size()) {
        // If the task is an actor task or an actor creation task,
        // put the dummy object in object store, so those tasks which depends on it
        // can be executed.
        putObject = new NativeRayObject(new byte[] {1}, null);
      } else {
        putObject = returnObjects.get(i);
      }
      objectStore.putRaw(putObject, returnIds.get(i));
    }
  }

  private static JavaFunctionDescriptor getJavaFunctionDescriptor(TaskSpec taskSpec) {
    Common.FunctionDescriptor functionDescriptor = taskSpec.getFunctionDescriptor();
    if (functionDescriptor.getFunctionDescriptorCase()
        == Common.FunctionDescriptor.FunctionDescriptorCase.JAVA_FUNCTION_DESCRIPTOR) {
      return new JavaFunctionDescriptor(
          functionDescriptor.getJavaFunctionDescriptor().getClassName(),
          functionDescriptor.getJavaFunctionDescriptor().getFunctionName(),
          functionDescriptor.getJavaFunctionDescriptor().getSignature());
    } else {
      throw new RuntimeException("Can't build non java function descriptor");
    }
  }

  private static List<FunctionArg> getFunctionArgs(TaskSpec taskSpec) {
    List<FunctionArg> functionArgs = new ArrayList<>();
    for (int i = 0; i < taskSpec.getArgsCount(); i++) {
      TaskArg arg = taskSpec.getArgs(i);
      if (arg.getObjectRef().getObjectId() != ByteString.EMPTY) {
        functionArgs.add(
            FunctionArg.passByReference(
                new ObjectId(arg.getObjectRef().getObjectId().toByteArray()),
                Address.getDefaultInstance()));
      } else {
        functionArgs.add(
            FunctionArg.passByValue(
                new NativeRayObject(arg.getData().toByteArray(), arg.getMetadata().toByteArray())));
      }
    }
    return functionArgs;
  }

  private static List<ObjectId> getReturnIds(TaskSpec taskSpec) {
    return getReturnIds(
        TaskId.fromBytes(taskSpec.getTaskId().toByteArray()), taskSpec.getNumReturns());
  }

  private static List<ObjectId> getReturnIds(TaskId taskId, long numReturns) {
    List<ObjectId> returnIds = new ArrayList<>();
    for (int i = 0; i < numReturns; i++) {
      returnIds.add(
          ObjectId.fromByteBuffer(
              (ByteBuffer)
                  ByteBuffer.allocate(ObjectId.LENGTH)
                      .put(taskId.getBytes())
                      .putInt(TaskId.LENGTH, i + 1)
                      .position(0)));
    }
    return returnIds;
  }

  /** Whether this is an concurrent actor. */
  private boolean isConcurrentActor(TaskSpec taskSpec) {
    final ActorId actorId = getActorId(taskSpec);
    Preconditions.checkNotNull(actorId);
    return actorMaxConcurrency.containsKey(actorId) && actorMaxConcurrency.get(actorId) > 1;
  }
}
