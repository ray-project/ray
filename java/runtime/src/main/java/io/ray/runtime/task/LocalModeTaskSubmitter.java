package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.ConcurrencyGroupImpl;
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
import io.ray.runtime.util.IdUtil;
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

  private final Map<ActorId, Integer> actorMaxConcurrency = new ConcurrentHashMap<>();

  /// The thread pool to execute normal tasks.
  private final ExecutorService normalTaskExecutorService;

  private final Map<ActorId, LocalModeActorHandle> actorHandles = new ConcurrentHashMap<>();

  private final Map<String, ActorHandle> namedActors = new ConcurrentHashMap<>();

  private final Map<ActorId, TaskExecutor.ActorContext> actorContexts = new ConcurrentHashMap<>();

  private final Map<PlacementGroupId, PlacementGroup> placementGroups = new ConcurrentHashMap<>();

  private static final String DEFAULT_CONCURRENCY_GROUP_NAME = "DEFAULT_CONCURRENCY_GROUP_NAME";

  private final ActorConcurrencyGroupManager actorConcurrencyGroupManager;

  private static final class ActorExecutorService {

    private Map<String, ExecutorService> services = new ConcurrentHashMap<>();

    /// A map that index the actor functions to its concurrency group.
    private Map<JavaFunctionDescriptor, String> indexFunctionToConcurrencyGroupName =
        new ConcurrentHashMap<>();

    public ActorExecutorService(TaskSpec taskSpec) {
      ActorCreationTaskSpec actorCreationTaskSpec = taskSpec.getActorCreationTaskSpec();
      Preconditions.checkNotNull(actorCreationTaskSpec);
      final List<Common.ConcurrencyGroup> concurrencyGroups =
          actorCreationTaskSpec.getConcurrencyGroupsList();
      concurrencyGroups.forEach(
          (concurrencyGroup) -> {
            ExecutorService executorService =
                Executors.newFixedThreadPool(concurrencyGroup.getMaxConcurrency());
            Preconditions.checkState(!services.containsKey(concurrencyGroup.getName()));
            services.put(concurrencyGroup.getName(), executorService);
            concurrencyGroup
                .getFunctionDescriptorsList()
                .forEach(
                    (fd) -> {
                      indexFunctionToConcurrencyGroupName.put(
                          protoFunctionDescriptorToJava(fd), concurrencyGroup.getName());
                    });
          });

      /// Put the default concurrency group.
      services.put(
          /*defaultConcurrencyGroupName=*/ DEFAULT_CONCURRENCY_GROUP_NAME,
          Executors.newFixedThreadPool(actorCreationTaskSpec.getMaxConcurrency()));
    }

    public synchronized ExecutorService getExecutorService(TaskSpec taskSpec) {
      String concurrencyGroupName = taskSpec.getConcurrencyGroupName();
      Preconditions.checkNotNull(concurrencyGroupName);
      /// First look up it by the given concurrency group name.
      if (!concurrencyGroupName.isEmpty()) {
        Preconditions.checkState(services.containsKey(concurrencyGroupName));
        return services.get(concurrencyGroupName);
      }
      /// The concurrency group is not specified, then we look up it by the function name.
      JavaFunctionDescriptor javaFunctionDescriptor =
          protoFunctionDescriptorToJava(taskSpec.getFunctionDescriptor());
      if (indexFunctionToConcurrencyGroupName.containsKey(javaFunctionDescriptor)) {
        concurrencyGroupName = indexFunctionToConcurrencyGroupName.get(javaFunctionDescriptor);
        Preconditions.checkState(services.containsKey(concurrencyGroupName));
        return services.get(concurrencyGroupName);
      } else {
        /// This function is not specified any concurrency group both in creating actor and
        // submitting task.
        return services.get(DEFAULT_CONCURRENCY_GROUP_NAME);
      }
    }

    public synchronized void shutdown() {
      services.forEach((key, service) -> service.shutdown());
      services.clear();
    }
  }

  private static final class ActorConcurrencyGroupManager {

    private Map<ActorId, ActorExecutorService> actorExecutorServices = new ConcurrentHashMap<>();

    public synchronized void registerActor(ActorId actorId, TaskSpec taskSpec) {
      Preconditions.checkState(!actorExecutorServices.containsKey(actorId));
      ActorExecutorService actorExecutorService = new ActorExecutorService(taskSpec);
      actorExecutorServices.put(actorId, actorExecutorService);
    }

    public synchronized ExecutorService getExecutorServiceForConcurrencyGroup(TaskSpec taskSpec) {
      final ActorId actorId = getActorId(taskSpec);
      Preconditions.checkState(actorExecutorServices.containsKey(actorId));
      ActorExecutorService actorExecutorService = actorExecutorServices.get(actorId);
      return actorExecutorService.getExecutorService(taskSpec);
    }

    public synchronized void shutdown() {
      actorExecutorServices.forEach(
          (actorId, actorExecutorService) -> {
            actorExecutorService.shutdown();
          });
      actorExecutorServices.clear();
    }
  }

  public LocalModeTaskSubmitter(
      RayRuntimeInternal runtime, TaskExecutor taskExecutor, LocalModeObjectStore objectStore) {
    this.runtime = runtime;
    this.taskExecutor = taskExecutor;
    this.objectStore = objectStore;
    // The thread pool that executes normal tasks in parallel.
    normalTaskExecutorService = Executors.newCachedThreadPool();
    actorConcurrencyGroupManager = new ActorConcurrencyGroupManager();
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
    if (taskSpec.getType() == TaskType.ACTOR_TASK && !isConcurrentActor(taskSpec)) {
      ObjectId dummyObjectId =
          new ObjectId(
              taskSpec.getActorTaskSpec().getPreviousActorTaskDummyObjectId().toByteArray());
      if (!objectStore.isObjectReady(dummyObjectId)) {
        unreadyObjects.add(dummyObjectId);
      }
    } else if (taskSpec.getType() == TaskType.ACTOR_TASK) {
      // Code path of concurrent actors.
      // For concurrent actors, we should make sure the actor created
      // before we submit the following actor tasks.
      ActorId actorId = ActorId.fromBytes(taskSpec.getActorTaskSpec().getActorId().toByteArray());
      ObjectId dummyActorCreationObjectId = IdUtil.getActorCreationDummyObjectId(actorId);
      if (!objectStore.isObjectReady(dummyActorCreationObjectId)) {
        unreadyObjects.add(dummyActorCreationObjectId);
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
        // bundleIndex == -1 indicates using any available bundle.
        Preconditions.checkArgument(
            options.bundleIndex == -1
                || options.bundleIndex >= 0 && options.bundleIndex < group.getBundles().size(),
            String.format(
                "Bundle index %s is invalid, the correct bundle index should be "
                    + "either in the range of 0 to the number of bundles "
                    + "or -1 which means put the task to any available bundles.",
                options.bundleIndex));
      }
    }

    ActorId actorId = ActorId.fromRandom();
    ActorCreationTaskSpec.Builder actorCreationTaskSpecBuilder =
        ActorCreationTaskSpec.newBuilder()
            .setActorId(ByteString.copyFrom(actorId.toByteBuffer()))
            .setMaxConcurrency(options.maxConcurrency)
            .setMaxPendingCalls(options.maxPendingCalls);
    appendConcurrencyGroupsBuilder(actorCreationTaskSpecBuilder, options);
    TaskSpec taskSpec =
        getTaskSpecBuilder(TaskType.ACTOR_CREATION_TASK, functionDescriptor, args)
            .setNumReturns(1)
            .setActorCreationTaskSpec(actorCreationTaskSpecBuilder.build())
            .build();
    submitTaskSpec(taskSpec);
    final LocalModeActorHandle actorHandle =
        new LocalModeActorHandle(actorId, getReturnIds(taskSpec).get(0));
    actorHandles.put(actorId, actorHandle.copy());
    if (StringUtils.isNotBlank(options.name)) {
      Preconditions.checkArgument(
          !namedActors.containsKey(options.name),
          String.format("Actor of name %s exists", options.name));
      namedActors.put(options.name, actorHandle);
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
            .setConcurrencyGroupName(options.concurrencyGroupName)
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
  public boolean waitPlacementGroupReady(PlacementGroupId id, int timeoutSeconds) {
    return true;
  }

  @Override
  public BaseActorHandle getActor(ActorId actorId) {
    return actorHandles.get(actorId).copy();
  }

  public Optional<BaseActorHandle> getActor(String name) {
    ActorHandle actorHandle = namedActors.get(name);
    if (null == actorHandle) {
      return Optional.empty();
    }
    return Optional.of(actorHandle);
  }

  public void shutdown() {
    // Shutdown actor concurrency group manager.
    actorConcurrencyGroupManager.shutdown();
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
              if (taskSpec.getType() == TaskType.ACTOR_CREATION_TASK) {
                // Construct a dummy object id for actor creation task so that the following
                // actor task can touch if this actor is created.
                ObjectId dummy =
                    IdUtil.getActorCreationDummyObjectId(
                        ActorId.fromBytes(
                            taskSpec.getActorCreationTaskSpec().getActorId().toByteArray()));
                objectStore.put(new Object(), dummy);
              }
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
          synchronized (actorConcurrencyGroupManager) {
            actorConcurrencyGroupManager.registerActor(getActorId(taskSpec), taskSpec);
          }
          executorService = normalTaskExecutorService;
        } else if (taskSpec.getType() == TaskType.ACTOR_TASK) {
          synchronized (actorConcurrencyGroupManager) {
            executorService =
                actorConcurrencyGroupManager.getExecutorServiceForConcurrencyGroup(taskSpec);
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
      final TaskExecutor.ActorContext ac = taskExecutor.getActorContext();
      Preconditions.checkNotNull(ac);
      actorContexts.put(getActorId(taskSpec), ac);
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

  /** Whether this is a concurrent actor. */
  private boolean isConcurrentActor(TaskSpec taskSpec) {
    final ActorId actorId = getActorId(taskSpec);
    Preconditions.checkNotNull(actorId);
    return actorMaxConcurrency.containsKey(actorId) && actorMaxConcurrency.get(actorId) > 1;
  }

  private static void appendConcurrencyGroupsBuilder(
      ActorCreationTaskSpec.Builder actorCreationTaskSpecBuilder, ActorCreationOptions options) {
    Preconditions.checkNotNull(actorCreationTaskSpecBuilder);
    if (options == null
        || options.concurrencyGroups == null
        || options.concurrencyGroups.isEmpty()) {
      return;
    }

    options.concurrencyGroups.forEach(
        (concurrencyGroup) -> {
          Common.ConcurrencyGroup.Builder concurrencyGroupBuilder =
              Common.ConcurrencyGroup.newBuilder();
          ConcurrencyGroupImpl impl = (ConcurrencyGroupImpl) concurrencyGroup;
          concurrencyGroupBuilder
              .setMaxConcurrency(impl.getMaxConcurrency())
              .setName(impl.getName());
          appendFunctionDescriptors(concurrencyGroupBuilder, impl.getFunctionDescriptors());
          actorCreationTaskSpecBuilder.addConcurrencyGroups(concurrencyGroupBuilder);
        });
  }

  private static void appendFunctionDescriptors(
      Common.ConcurrencyGroup.Builder builder, List<FunctionDescriptor> functionDescriptors) {
    Preconditions.checkNotNull(functionDescriptors);
    Preconditions.checkState(!functionDescriptors.isEmpty());
    functionDescriptors.stream()
        .map(functionDescriptor -> (JavaFunctionDescriptor) functionDescriptor)
        .map(
            javaFunctionDescriptor ->
                Common.FunctionDescriptor.newBuilder()
                    .setJavaFunctionDescriptor(
                        Common.JavaFunctionDescriptor.newBuilder()
                            .setClassName(javaFunctionDescriptor.className)
                            .setFunctionName(javaFunctionDescriptor.name)
                            .setSignature(javaFunctionDescriptor.signature)))
        .forEach(builder::addFunctionDescriptors);
  }

  private static JavaFunctionDescriptor protoFunctionDescriptorToJava(
      Common.FunctionDescriptor protoFunctionDescriptor) {
    Preconditions.checkNotNull(protoFunctionDescriptor);
    Common.JavaFunctionDescriptor protoJavaFunctionDescriptor =
        protoFunctionDescriptor.getJavaFunctionDescriptor();
    return new JavaFunctionDescriptor(
        protoJavaFunctionDescriptor.getClassName(),
        protoJavaFunctionDescriptor.getFunctionName(),
        protoJavaFunctionDescriptor.getSignature());
  }
}
