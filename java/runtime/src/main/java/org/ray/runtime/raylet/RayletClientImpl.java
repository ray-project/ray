package org.ray.runtime.raylet;

import com.google.common.base.Preconditions;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.exception.RayException;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.generated.Arg;
import org.ray.runtime.generated.Language;
import org.ray.runtime.generated.ResourcePair;
import org.ray.runtime.generated.TaskInfo;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskLanguage;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RayletClientImpl implements RayletClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayletClientImpl.class);

  private static final int TASK_SPEC_BUFFER_SIZE = 2 * 1024 * 1024;

  /**
   * Direct buffers that are used to hold flatbuffer-serialized task specs.
   */
  private static ThreadLocal<ByteBuffer> taskSpecBuffer = ThreadLocal.withInitial(() ->
      ByteBuffer.allocateDirect(TASK_SPEC_BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN)
  );

  /**
   * The pointer to c++'s raylet client.
   */
  private long client = 0;

  public RayletClientImpl(String schedulerSockName, UniqueId clientId,
      boolean isWorker, UniqueId driverId) {
    client = nativeInit(schedulerSockName, clientId.getBytes(),
        isWorker, driverId.getBytes());
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
    Preconditions.checkState(!spec.driverId.isNil());

    ByteBuffer info = convertTaskSpecToFlatbuffer(spec);
    byte[] cursorId = null;
    if (!spec.getExecutionDependencies().isEmpty()) {
      //TODO(hchen): handle more than one dependencies.
      cursorId = spec.getExecutionDependencies().get(0).getBytes();
    }
    nativeSubmitTask(client, cursorId, info, info.position(), info.remaining());
  }

  @Override
  public TaskSpec getTask() {
    byte[] bytes = nativeGetTask(client);
    assert (null != bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return parseTaskSpecFromFlatbuffer(bb);
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
  public TaskId generateTaskId(UniqueId driverId, TaskId parentTaskId, int taskIndex) {
    byte[] bytes = nativeGenerateTaskId(driverId.getBytes(), parentTaskId.getBytes(), taskIndex);
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


  private static TaskSpec parseTaskSpecFromFlatbuffer(ByteBuffer bb) {
    bb.order(ByteOrder.LITTLE_ENDIAN);
    TaskInfo info = TaskInfo.getRootAsTaskInfo(bb);
    UniqueId driverId = UniqueId.fromByteBuffer(info.driverIdAsByteBuffer());
    TaskId taskId = TaskId.fromByteBuffer(info.taskIdAsByteBuffer());
    TaskId parentTaskId = TaskId.fromByteBuffer(info.parentTaskIdAsByteBuffer());
    int parentCounter = info.parentCounter();
    UniqueId actorCreationId = UniqueId.fromByteBuffer(info.actorCreationIdAsByteBuffer());
    int maxActorReconstructions = info.maxActorReconstructions();
    UniqueId actorId = UniqueId.fromByteBuffer(info.actorIdAsByteBuffer());
    UniqueId actorHandleId = UniqueId.fromByteBuffer(info.actorHandleIdAsByteBuffer());
    int actorCounter = info.actorCounter();
    int numReturns = info.numReturns();

    // Deserialize new actor handles
    UniqueId[] newActorHandles = IdUtil.getUniqueIdsFromByteBuffer(
        info.newActorHandlesAsByteBuffer());

    // Deserialize args
    FunctionArg[] args = new FunctionArg[info.argsLength()];
    for (int i = 0; i < info.argsLength(); i++) {
      Arg arg = info.args(i);

      int objectIdsLength = arg.objectIdsAsByteBuffer().remaining() / UniqueId.LENGTH;
      if (objectIdsLength > 0) {
        Preconditions.checkArgument(objectIdsLength == 1,
            "This arg has more than one id: {}", objectIdsLength);
        args[i] = FunctionArg.passByReference(ObjectId.fromByteBuffer(arg.objectIdsAsByteBuffer()));
      } else {
        ByteBuffer lbb = arg.dataAsByteBuffer();
        Preconditions.checkState(lbb != null && lbb.remaining() > 0);
        byte[] data = new byte[lbb.remaining()];
        lbb.get(data);
        args[i] = FunctionArg.passByValue(data);
      }
    }

    // Deserialize required resources;
    Map<String, Double> resources = new HashMap<>();
    for (int i = 0; i < info.requiredResourcesLength(); i++) {
      resources.put(info.requiredResources(i).key(), info.requiredResources(i).value());
    }
    // Deserialize function descriptor
    Preconditions.checkArgument(info.language() == Language.JAVA);
    Preconditions.checkArgument(info.functionDescriptorLength() == 3);
    JavaFunctionDescriptor functionDescriptor = new JavaFunctionDescriptor(
        info.functionDescriptor(0), info.functionDescriptor(1), info.functionDescriptor(2)
    );
    return new TaskSpec(driverId, taskId, parentTaskId, parentCounter, actorCreationId,
        maxActorReconstructions, actorId, actorHandleId, actorCounter, newActorHandles,
        args, numReturns, resources, TaskLanguage.JAVA, functionDescriptor);
  }

  private static ByteBuffer convertTaskSpecToFlatbuffer(TaskSpec task) {
    ByteBuffer bb = taskSpecBuffer.get();
    bb.clear();

    FlatBufferBuilder fbb = new FlatBufferBuilder(bb);
    final int driverIdOffset = fbb.createString(task.driverId.toByteBuffer());
    final int taskIdOffset = fbb.createString(task.taskId.toByteBuffer());
    final int parentTaskIdOffset = fbb.createString(task.parentTaskId.toByteBuffer());
    final int parentCounter = task.parentCounter;
    final int actorCreateIdOffset = fbb.createString(task.actorCreationId.toByteBuffer());
    final int actorCreateDummyIdOffset = fbb.createString(task.actorId.toByteBuffer());
    final int maxActorReconstructions = task.maxActorReconstructions;
    final int actorIdOffset = fbb.createString(task.actorId.toByteBuffer());
    final int actorHandleIdOffset = fbb.createString(task.actorHandleId.toByteBuffer());
    final int actorCounter = task.actorCounter;
    final int numReturnsOffset = task.numReturns;

    // Serialize the new actor handles.
    int newActorHandlesOffset
        = fbb.createString(IdUtil.concatIds(task.newActorHandles));

    // Serialize args
    int[] argsOffsets = new int[task.args.length];
    for (int i = 0; i < argsOffsets.length; i++) {
      int objectIdOffset = 0;
      int dataOffset = 0;
      if (task.args[i].id != null) {
        objectIdOffset = fbb.createString(
            IdUtil.concatIds(new ObjectId[]{task.args[i].id}));
      } else {
        objectIdOffset = fbb.createString("");
      }
      if (task.args[i].data != null) {
        dataOffset = fbb.createString(ByteBuffer.wrap(task.args[i].data));
      }
      argsOffsets[i] = Arg.createArg(fbb, objectIdOffset, dataOffset);
    }
    int argsOffset = fbb.createVectorOfTables(argsOffsets);

    // Serialize required resources
    // The required_resources vector indicates the quantities of the different
    // resources required by this task. The index in this vector corresponds to
    // the resource type defined in the ResourceIndex enum. For example,
    int[] requiredResourcesOffsets = new int[task.resources.size()];
    int i = 0;
    for (Map.Entry<String, Double> entry : task.resources.entrySet()) {
      int keyOffset = fbb.createString(ByteBuffer.wrap(entry.getKey().getBytes()));
      requiredResourcesOffsets[i++] =
          ResourcePair.createResourcePair(fbb, keyOffset, entry.getValue());
    }
    int requiredResourcesOffset = fbb.createVectorOfTables(requiredResourcesOffsets);

    int[] requiredPlacementResourcesOffsets = new int[0];
    int requiredPlacementResourcesOffset =
        fbb.createVectorOfTables(requiredPlacementResourcesOffsets);

    int language;
    int functionDescriptorOffset;

    if (task.language == TaskLanguage.JAVA) {
      // This is a Java task.
      language = Language.JAVA;
      int[] functionDescriptorOffsets = new int[]{
          fbb.createString(task.getJavaFunctionDescriptor().className),
          fbb.createString(task.getJavaFunctionDescriptor().name),
          fbb.createString(task.getJavaFunctionDescriptor().typeDescriptor)
      };
      functionDescriptorOffset = fbb.createVectorOfTables(functionDescriptorOffsets);
    } else {
      // This is a Python task.
      language = Language.PYTHON;
      int[] functionDescriptorOffsets = new int[]{
          fbb.createString(task.getPyFunctionDescriptor().moduleName),
          fbb.createString(task.getPyFunctionDescriptor().className),
          fbb.createString(task.getPyFunctionDescriptor().functionName),
          fbb.createString("")
      };
      functionDescriptorOffset = fbb.createVectorOfTables(functionDescriptorOffsets);
    }

    int root = TaskInfo.createTaskInfo(
        fbb,
        driverIdOffset,
        taskIdOffset,
        parentTaskIdOffset,
        parentCounter,
        actorCreateIdOffset,
        actorCreateDummyIdOffset,
        maxActorReconstructions,
        actorIdOffset,
        actorHandleIdOffset,
        actorCounter,
        newActorHandlesOffset,
        argsOffset,
        numReturnsOffset,
        requiredResourcesOffset,
        requiredPlacementResourcesOffset,
        language,
        functionDescriptorOffset);
    fbb.finish(root);
    ByteBuffer buffer = fbb.dataBuffer();

    if (buffer.remaining() > TASK_SPEC_BUFFER_SIZE) {
      LOGGER.error(
          "Allocated buffer is not enough to transfer the task specification: {} vs {}",
          TASK_SPEC_BUFFER_SIZE, buffer.remaining());
      throw new RuntimeException("Allocated buffer is not enough to transfer to task.");
    }
    return buffer;
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

  private static native void nativeSubmitTask(long client, byte[] cursorId, ByteBuffer taskBuff,
      int pos, int taskSize) throws RayException;

  // return TaskInfo (in FlatBuffer)
  private static native byte[] nativeGetTask(long client) throws RayException;

  private static native void nativeDestroy(long client) throws RayException;

  private static native void nativeFetchOrReconstruct(long client, byte[][] objectIds,
      boolean fetchOnly, byte[] currentTaskId) throws RayException;

  private static native void nativeNotifyUnblocked(long client, byte[] currentTaskId)
      throws RayException;

  private static native void nativePutObject(long client, byte[] taskId, byte[] objectId);

  private static native boolean[] nativeWaitObject(long conn, byte[][] objectIds,
      int numReturns, int timeout, boolean waitLocal, byte[] currentTaskId) throws RayException;

  private static native byte[] nativeGenerateTaskId(byte[] driverId, byte[] parentTaskId,
      int taskIndex);

  private static native void nativeFreePlasmaObjects(long conn, byte[][] objectIds,
      boolean localOnly, boolean deleteCreatingTasks) throws RayException;

  private static native byte[] nativePrepareCheckpoint(long conn, byte[] actorId);

  private static native void nativeNotifyActorResumedFromCheckpoint(long conn, byte[] actorId,
      byte[] checkpointId);

  private static native void nativeSetResource(long conn, String resourceName, double capacity,
                                               byte[] nodeId) throws RayException;
}
