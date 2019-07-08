package org.ray.runtime.raylet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.id.JobId;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.generated.Common;
import org.ray.runtime.generated.Common.TaskType;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskLanguage;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayletClientImpl implements RayletClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayletClientImpl.class);

  /**
   * The pointer to c++'s raylet client.
   */
  private long client = 0;

  // TODO(qwang): JobId parameter can be removed once we embed jobId in driverId.
  public RayletClientImpl(String schedulerSockName, UniqueId clientId,
                          boolean isWorker, JobId jobId) {
    client = nativeInit(schedulerSockName, clientId.getBytes(),
        isWorker, jobId.getBytes());
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int
      timeoutMs, TaskId currentTaskId) {
    Preconditions.checkNotNull(waitFor);
    if (waitFor.isEmpty()) {
      return new WaitResult<>(new ArrayList<>(), new ArrayList<>());
    }

    List<ObjectId> ids = new ArrayList<>();
    for (RayObject<T> element : waitFor) {
      ids.add(element.getId());
    }

    boolean[] ready = nativeWaitObject(client, IdUtil.getIdBytes(ids),
        numReturns, timeoutMs, false, currentTaskId.getBytes());
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();

    for (int i = 0; i < ready.length; i++) {
      if (ready[i]) {
        readyList.add(waitFor.get(i));
      } else {
        unreadyList.add(waitFor.get(i));
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public void submitTask(TaskSpec spec) {
    LOGGER.debug("Submitting task: {}", spec);
    Preconditions.checkState(!spec.parentTaskId.isNil());
    Preconditions.checkState(!spec.jobId.isNil());

    byte[] taskSpec = convertTaskSpecToProtobuf(spec);
    byte[] cursorId = null;
    if (!spec.getExecutionDependencies().isEmpty()) {
      //TODO(hchen): handle more than one dependencies.
      cursorId = spec.getExecutionDependencies().get(0).getBytes();
    }
    nativeSubmitTask(client, cursorId, taskSpec);
  }

  @Override
  public TaskSpec getTask() {
    byte[] bytes = nativeGetTask(client);
    assert (null != bytes);
    return parseTaskSpecFromProtobuf(bytes);
  }

  @Override
  public void fetchOrReconstruct(List<ObjectId> objectIds, boolean fetchOnly,
      TaskId currentTaskId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Blocked on objects for task {}, object IDs are {}",
          objectIds.get(0).getTaskId(), objectIds);
    }
    nativeFetchOrReconstruct(client, IdUtil.getIdBytes(objectIds),
        fetchOnly, currentTaskId.getBytes());
  }

  @Override
  public TaskId generateTaskId(JobId jobId, TaskId parentTaskId, int taskIndex) {
    byte[] bytes = nativeGenerateTaskId(jobId.getBytes(), parentTaskId.getBytes(), taskIndex);
    return new TaskId(bytes);
  }

  @Override
  public void notifyUnblocked(TaskId currentTaskId) {
    nativeNotifyUnblocked(client, currentTaskId.getBytes());
  }

  @Override
  public void freePlasmaObjects(List<ObjectId> objectIds, boolean localOnly,
      boolean deleteCreatingTasks) {
    byte[][] objectIdsArray = IdUtil.getIdBytes(objectIds);
    nativeFreePlasmaObjects(client, objectIdsArray, localOnly, deleteCreatingTasks);
  }

  @Override
  public UniqueId prepareCheckpoint(UniqueId actorId) {
    return new UniqueId(nativePrepareCheckpoint(client, actorId.getBytes()));
  }

  @Override
  public void notifyActorResumedFromCheckpoint(UniqueId actorId, UniqueId checkpointId) {
    nativeNotifyActorResumedFromCheckpoint(client, actorId.getBytes(), checkpointId.getBytes());
  }

  /**
   * Parse `TaskSpec` protobuf bytes.
   */
  private static TaskSpec parseTaskSpecFromProtobuf(byte[] bytes) {
    Common.TaskSpec taskSpec;
    try {
      taskSpec = Common.TaskSpec.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Invalid protobuf data.");
    }

    // Parse common fields.
    JobId jobId = JobId.fromByteBuffer(taskSpec.getJobId().asReadOnlyByteBuffer());
    TaskId taskId = TaskId.fromByteBuffer(taskSpec.getTaskId().asReadOnlyByteBuffer());
    TaskId parentTaskId = TaskId.fromByteBuffer(taskSpec.getParentTaskId().asReadOnlyByteBuffer());
    int parentCounter = (int) taskSpec.getParentCounter();
    int numReturns = (int) taskSpec.getNumReturns();
    Map<String, Double> resources = taskSpec.getRequiredResourcesMap();

    // Parse args.
    FunctionArg[] args = new FunctionArg[taskSpec.getArgsCount()];
    for (int i = 0; i < args.length; i++) {
      Common.TaskArg arg = taskSpec.getArgs(i);
      int objectIdsLength = arg.getObjectIdsCount();
      if (objectIdsLength > 0) {
        Preconditions.checkArgument(objectIdsLength == 1,
            "This arg has more than one id: {}", objectIdsLength);
        args[i] = FunctionArg
            .passByReference(ObjectId.fromByteBuffer(arg.getObjectIds(0).asReadOnlyByteBuffer()));
      } else {
        args[i] = FunctionArg.passByValue(arg.getData().toByteArray());
      }
    }

    // Parse function descriptor
    Preconditions.checkArgument(taskSpec.getLanguage() == Common.Language.JAVA);
    Preconditions.checkArgument(taskSpec.getFunctionDescriptorCount() == 3);
    JavaFunctionDescriptor functionDescriptor = new JavaFunctionDescriptor(
        taskSpec.getFunctionDescriptor(0).toString(Charset.defaultCharset()),
        taskSpec.getFunctionDescriptor(1).toString(Charset.defaultCharset()),
        taskSpec.getFunctionDescriptor(2).toString(Charset.defaultCharset())
    );

    // Parse ActorCreationTaskSpec.
    UniqueId actorCreationId = UniqueId.NIL;
    int maxActorReconstructions = 0;
    UniqueId[] newActorHandles = new UniqueId[0];
    List<String> dynamicWorkerOptions = new ArrayList<>();
    if (taskSpec.getType() == Common.TaskType.ACTOR_CREATION_TASK) {
      Common.ActorCreationTaskSpec actorCreationTaskSpec = taskSpec.getActorCreationTaskSpec();
      actorCreationId = UniqueId
          .fromByteBuffer(actorCreationTaskSpec.getActorId().asReadOnlyByteBuffer());
      maxActorReconstructions = (int) actorCreationTaskSpec.getMaxActorReconstructions();
      dynamicWorkerOptions = ImmutableList
          .copyOf(actorCreationTaskSpec.getDynamicWorkerOptionsList());
    }

    // Parse ActorTaskSpec.
    UniqueId actorId = UniqueId.NIL;
    UniqueId actorHandleId = UniqueId.NIL;
    int actorCounter = 0;
    if (taskSpec.getType() == Common.TaskType.ACTOR_TASK) {
      Common.ActorTaskSpec actorTaskSpec = taskSpec.getActorTaskSpec();
      actorId = UniqueId.fromByteBuffer(actorTaskSpec.getActorId().asReadOnlyByteBuffer());
      actorHandleId = UniqueId
          .fromByteBuffer(actorTaskSpec.getActorHandleId().asReadOnlyByteBuffer());
      actorCounter = (int) actorTaskSpec.getActorCounter();
      newActorHandles = actorTaskSpec.getNewActorHandlesList().stream()
          .map(byteString -> UniqueId.fromByteBuffer(byteString.asReadOnlyByteBuffer()))
          .toArray(UniqueId[]::new);
    }

    return new TaskSpec(jobId, taskId, parentTaskId, parentCounter, actorCreationId,
        maxActorReconstructions, actorId, actorHandleId, actorCounter, newActorHandles,
        args, numReturns, resources, TaskLanguage.JAVA, functionDescriptor, dynamicWorkerOptions);
  }

  /**
   * Convert a `TaskSpec` to protobuf-serialized bytes.
   */
  private static byte[] convertTaskSpecToProtobuf(TaskSpec task) {
    // Set common fields.
    Common.TaskSpec.Builder builder = Common.TaskSpec.newBuilder()
        .setJobId(ByteString.copyFrom(task.jobId.getBytes()))
        .setTaskId(ByteString.copyFrom(task.taskId.getBytes()))
        .setParentTaskId(ByteString.copyFrom(task.parentTaskId.getBytes()))
        .setParentCounter(task.parentCounter)
        .setNumReturns(task.numReturns)
        .putAllRequiredResources(task.resources);

    // Set args
    builder.addAllArgs(
        Arrays.stream(task.args).map(arg -> {
          Common.TaskArg.Builder argBuilder = Common.TaskArg.newBuilder();
          if (arg.id != null) {
            argBuilder.addObjectIds(ByteString.copyFrom(arg.id.getBytes())).build();
          } else {
            argBuilder.setData(ByteString.copyFrom(arg.data)).build();
          }
          return argBuilder.build();
        }).collect(Collectors.toList())
    );

    // Set function descriptor and language.
    if (task.language == TaskLanguage.JAVA) {
      builder.setLanguage(Common.Language.JAVA);
      builder.addAllFunctionDescriptor(ImmutableList.of(
          ByteString.copyFrom(task.getJavaFunctionDescriptor().className.getBytes()),
          ByteString.copyFrom(task.getJavaFunctionDescriptor().name.getBytes()),
          ByteString.copyFrom(task.getJavaFunctionDescriptor().typeDescriptor.getBytes())
      ));
    } else {
      builder.setLanguage(Common.Language.PYTHON);
      builder.addAllFunctionDescriptor(ImmutableList.of(
          ByteString.copyFrom(task.getPyFunctionDescriptor().moduleName.getBytes()),
          ByteString.copyFrom(task.getPyFunctionDescriptor().className.getBytes()),
          ByteString.copyFrom(task.getPyFunctionDescriptor().functionName.getBytes()),
          ByteString.EMPTY
      ));
    }

    if (!task.actorCreationId.isNil()) {
      // Actor creation task.
      builder.setType(TaskType.ACTOR_CREATION_TASK);
      builder.setActorCreationTaskSpec(
          Common.ActorCreationTaskSpec.newBuilder()
              .setActorId(ByteString.copyFrom(task.actorCreationId.getBytes()))
              .setMaxActorReconstructions(task.maxActorReconstructions)
              .addAllDynamicWorkerOptions(task.dynamicWorkerOptions)
      );
    } else if (!task.actorId.isNil()) {
      // Actor task.
      builder.setType(TaskType.ACTOR_TASK);
      List<ByteString> newHandles = Arrays.stream(task.newActorHandles)
          .map(id -> ByteString.copyFrom(id.getBytes())).collect(Collectors.toList());
      builder.setActorTaskSpec(
          Common.ActorTaskSpec.newBuilder()
              .setActorId(ByteString.copyFrom(task.actorId.getBytes()))
              .setActorHandleId(ByteString.copyFrom(task.actorHandleId.getBytes()))
              .setActorCreationDummyObjectId(ByteString.copyFrom(task.actorId.getBytes()))
              .setActorCounter(task.actorCounter)
              .addAllNewActorHandles(newHandles)
      );
    } else {
      // Normal task.
      builder.setType(TaskType.NORMAL_TASK);
    }

    return builder.build().toByteArray();
  }

  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    nativeSetResource(client, resourceName, capacity, nodeId.getBytes());
  }

  public void destroy() {
    nativeDestroy(client);
  }

  /// Native method declarations.
  ///
  /// If you change the signature of any native methods, please re-generate
  /// the C++ header file and update the C++ implementation accordingly:
  ///
  /// Suppose that $Dir is your ray root directory.
  /// 1) pushd $Dir/java/runtime/target/classes
  /// 2) javah -classpath .:$Dir/java/api/target/classes org.ray.runtime.raylet.RayletClientImpl
  /// 3) clang-format -i org_ray_runtime_raylet_RayletClientImpl.h
  /// 4) cp org_ray_runtime_raylet_RayletClientImpl.h $Dir/src/ray/raylet/lib/java/
  /// 5) vim $Dir/src/ray/raylet/lib/java/org_ray_runtime_raylet_RayletClientImpl.cc
  /// 6) popd

  private static native long nativeInit(String localSchedulerSocket, byte[] workerId,
      boolean isWorker, byte[] driverTaskId);

  private static native void nativeSubmitTask(long client, byte[] cursorId, byte[] taskSpec)
      throws RayException;

  private static native byte[] nativeGetTask(long client) throws RayException;

  private static native void nativeDestroy(long client) throws RayException;

  private static native void nativeFetchOrReconstruct(long client, byte[][] objectIds,
      boolean fetchOnly, byte[] currentTaskId) throws RayException;

  private static native void nativeNotifyUnblocked(long client, byte[] currentTaskId)
      throws RayException;

  private static native void nativePutObject(long client, byte[] taskId, byte[] objectId);

  private static native boolean[] nativeWaitObject(long conn, byte[][] objectIds,
      int numReturns, int timeout, boolean waitLocal, byte[] currentTaskId) throws RayException;

  private static native byte[] nativeGenerateTaskId(byte[] jobId, byte[] parentTaskId,
      int taskIndex);

  private static native void nativeFreePlasmaObjects(long conn, byte[][] objectIds,
      boolean localOnly, boolean deleteCreatingTasks) throws RayException;

  private static native byte[] nativePrepareCheckpoint(long conn, byte[] actorId);

  private static native void nativeNotifyActorResumedFromCheckpoint(long conn, byte[] actorId,
      byte[] checkpointId);

  private static native void nativeSetResource(long conn, String resourceName, double capacity,
      byte[] nodeId) throws RayException;
}
