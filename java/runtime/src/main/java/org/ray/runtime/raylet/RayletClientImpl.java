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
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.generated.Arg;
import org.ray.runtime.generated.Language;
import org.ray.runtime.generated.ResourcePair;
import org.ray.runtime.generated.TaskInfo;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;
import org.ray.runtime.util.UniqueIdUtil;
import org.ray.runtime.util.logger.RayLog;

public class RayletClientImpl implements RayletClient {

  private static final int TASK_SPEC_BUFFER_SIZE = 2 * 1024 * 1024;

  /**
   * Direct buffers that are used to hold flatbuffer-serialized task specs.
   */
  private static ThreadLocal<ByteBuffer> taskSpecBuffer = ThreadLocal.withInitial(() ->
      ByteBuffer.allocateDirect(TASK_SPEC_BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN)
  );

  /**
   * Point to c++'s local scheduler client.
   */
  private long client = 0;

  public RayletClientImpl(String schedulerSockName, UniqueId clientId,
      boolean isWorker, UniqueId driverId) {
    client = nativeInit(schedulerSockName, clientId.getBytes(),
        isWorker, driverId.getBytes());
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int
      timeoutMs, UniqueId currentTaskId) {
    List<UniqueId> ids = new ArrayList<>();
    for (RayObject<T> element : waitFor) {
      ids.add(element.getId());
    }

    boolean[] ready = nativeWaitObject(client, UniqueIdUtil.getIdBytes(ids),
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
    RayLog.core.debug("Submitting task: {}", spec);
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
  public void fetchOrReconstruct(List<UniqueId> objectIds, boolean fetchOnly,
      UniqueId currentTaskId) {
    if (RayLog.core.isInfoEnabled()) {
      RayLog.core.info("Blocked on objects for task {}, object IDs are {}",
          UniqueIdUtil.computeTaskId(objectIds.get(0)), objectIds);
    }
    nativeFetchOrReconstruct(client, UniqueIdUtil.getIdBytes(objectIds),
        fetchOnly, currentTaskId.getBytes());
  }

  @Override
  public UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex) {
    byte[] bytes = nativeGenerateTaskId(driverId.getBytes(), parentTaskId.getBytes(), taskIndex);
    return new UniqueId(bytes);
  }

  @Override
  public void notifyUnblocked(UniqueId currentTaskId) {
    nativeNotifyUnblocked(client, currentTaskId.getBytes());
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    byte[][] objectIdsArray = UniqueIdUtil.getIdBytes(objectIds);
    nativeFreePlasmaObjects(client, objectIdsArray, localOnly);
  }

  private static TaskSpec parseTaskSpecFromFlatbuffer(ByteBuffer bb) {
    bb.order(ByteOrder.LITTLE_ENDIAN);
    TaskInfo info = TaskInfo.getRootAsTaskInfo(bb);
    UniqueId driverId = UniqueId.fromByteBuffer(info.driverIdAsByteBuffer());
    UniqueId taskId = UniqueId.fromByteBuffer(info.taskIdAsByteBuffer());
    UniqueId parentTaskId = UniqueId.fromByteBuffer(info.parentTaskIdAsByteBuffer());
    int parentCounter = info.parentCounter();
    UniqueId actorCreationId = UniqueId.fromByteBuffer(info.actorCreationIdAsByteBuffer());
    UniqueId actorId = UniqueId.fromByteBuffer(info.actorIdAsByteBuffer());
    UniqueId actorHandleId = UniqueId.fromByteBuffer(info.actorHandleIdAsByteBuffer());
    int actorCounter = info.actorCounter();
    // Deserialize args
    FunctionArg[] args = new FunctionArg[info.argsLength()];
    for (int i = 0; i < info.argsLength(); i++) {
      Arg arg = info.args(i);
      if (arg.objectIdsLength() > 0) {
        Preconditions.checkArgument(arg.objectIdsLength() == 1,
            "This arg has more than one id: {}", arg.objectIdsLength());
        UniqueId id = UniqueId.fromByteBuffer(arg.objectIdAsByteBuffer(0));
        args[i] = FunctionArg.passByReference(id);
      } else {
        ByteBuffer lbb = arg.dataAsByteBuffer();
        Preconditions.checkState(lbb != null && lbb.remaining() > 0);
        byte[] data = new byte[lbb.remaining()];
        lbb.get(data);
        args[i] = FunctionArg.passByValue(data);
      }
    }
    // Deserialize return ids
    UniqueId[] returnIds = new UniqueId[info.returnsLength()];
    for (int i = 0; i < info.returnsLength(); i++) {
      returnIds[i] = UniqueId.fromByteBuffer(info.returnsAsByteBuffer(i));
    }
    // Deserialize required resources;
    Map<String, Double> resources = new HashMap<>();
    for (int i = 0; i < info.requiredResourcesLength(); i++) {
      resources.put(info.requiredResources(i).key(), info.requiredResources(i).value());
    }
    // Deserialize function descriptor
    Preconditions.checkArgument(info.functionDescriptorLength() == 3);
    FunctionDescriptor functionDescriptor = new FunctionDescriptor(
        info.functionDescriptor(0), info.functionDescriptor(1), info.functionDescriptor(2)
    );
    return new TaskSpec(driverId, taskId, parentTaskId, parentCounter, actorCreationId, actorId,
        actorHandleId, actorCounter, args, returnIds, resources, functionDescriptor);
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
    final int actorCreateDummyIdOffset = fbb.createString(UniqueId.NIL.toByteBuffer());
    final int actorIdOffset = fbb.createString(task.actorId.toByteBuffer());
    final int actorHandleIdOffset = fbb.createString(task.actorHandleId.toByteBuffer());
    final int actorCounter = task.actorCounter;
    final int functionIdOffset = fbb.createString(UniqueId.NIL.toByteBuffer());
    // Serialize args
    int[] argsOffsets = new int[task.args.length];
    for (int i = 0; i < argsOffsets.length; i++) {
      int objectIdOffset = 0;
      int dataOffset = 0;
      if (task.args[i].id != null) {
        int[] idOffsets = new int[]{fbb.createString(task.args[i].id.toByteBuffer())};
        objectIdOffset = fbb.createVectorOfTables(idOffsets);
      } else {
        objectIdOffset = fbb.createVectorOfTables(new int[0]);
      }
      if (task.args[i].data != null) {
        dataOffset = fbb.createString(ByteBuffer.wrap(task.args[i].data));
      }
      argsOffsets[i] = Arg.createArg(fbb, objectIdOffset, dataOffset);
    }
    int argsOffset = fbb.createVectorOfTables(argsOffsets);
    // Serialize returns
    int returnCount = task.returnIds.length;
    int[] returnsOffsets = new int[returnCount];
    for (int k = 0; k < returnCount; k++) {
      returnsOffsets[k] = fbb.createString(task.returnIds[k].toByteBuffer());
    }
    int returnsOffset = fbb.createVectorOfTables(returnsOffsets);
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

    int[] functionDescriptorOffsets = new int[]{
        fbb.createString(task.functionDescriptor.className),
        fbb.createString(task.functionDescriptor.name),
        fbb.createString(task.functionDescriptor.typeDescriptor)
    };
    int functionDescriptorOffset = fbb.createVectorOfTables(functionDescriptorOffsets);

    int root = TaskInfo.createTaskInfo(
        fbb, driverIdOffset, taskIdOffset,
        parentTaskIdOffset, parentCounter,
        actorCreateIdOffset, actorCreateDummyIdOffset,
        actorIdOffset, actorHandleIdOffset, actorCounter,
        false, functionIdOffset,
        argsOffset, returnsOffset, requiredResourcesOffset,
        requiredPlacementResourcesOffset, Language.JAVA,
        functionDescriptorOffset);
    fbb.finish(root);
    ByteBuffer buffer = fbb.dataBuffer();

    if (buffer.remaining() > TASK_SPEC_BUFFER_SIZE) {
      RayLog.core.error(
          "Allocated buffer is not enough to transfer the task specification: "
              + TASK_SPEC_BUFFER_SIZE + " vs " + buffer.remaining());
      assert (false);
    }
    return buffer;
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
      int pos, int taskSize);

  // return TaskInfo (in FlatBuffer)
  private static native byte[] nativeGetTask(long client);

  private static native void nativeDestroy(long client);

  private static native void nativeFetchOrReconstruct(long client, byte[][] objectIds,
      boolean fetchOnly, byte[] currentTaskId);

  private static native void nativeNotifyUnblocked(long client, byte[] currentTaskId);

  private static native void nativePutObject(long client, byte[] taskId, byte[] objectId);

  private static native boolean[] nativeWaitObject(long conn, byte[][] objectIds,
      int numReturns, int timeout, boolean waitLocal, byte[] currentTaskId);

  private static native byte[] nativeGenerateTaskId(byte[] driverId, byte[] parentTaskId,
      int taskIndex);

  private static native void nativeFreePlasmaObjects(long conn, byte[][] objectIds,
      boolean localOnly);

}
