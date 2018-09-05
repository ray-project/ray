package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.core.AbstractRayRuntime;
import org.ray.core.UniqueIdHelper;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.TaskSpec;
import org.ray.util.ResourceUtil;
import org.ray.util.logger.RayLog;

/**
 * JNI-based local scheduler link provider.
 */
public class DefaultLocalSchedulerClient implements LocalSchedulerLink {

  private static ThreadLocal<ByteBuffer> _taskBuffer = ThreadLocal.withInitial(() -> {
    ByteBuffer bb = ByteBuffer
        .allocateDirect(AbstractRayRuntime.getParams().max_submit_task_buffer_size_bytes);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return bb;
  });
  private long client = 0;

  public DefaultLocalSchedulerClient(String schedulerSockName, UniqueId clientId,
      boolean isWorker, UniqueId driverId) {
    client = nativeInit(schedulerSockName, clientId.getBytes(),
        isWorker, driverId.getBytes());
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitFor, int numReturns, int timeoutMillis) {
    List<UniqueId> ids = new ArrayList<>();
    for (RayObject<T> element : waitFor) {
      ids.add(element.getId());
    }

    boolean[] readys = nativeWaitObject(client, getIdBytes(ids), numReturns, timeoutMillis, false);
    if (readys.length != ids.size()) {
      throw new RuntimeException("Wait for objects failed.");
    }

    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();

    for (int i = 0; i < readys.length; i++) {
      if (readys[i]) {
        readyList.add(waitFor.get(i));
      } else {
        unreadyList.add(waitFor.get(i));
      }
    }

    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public void submitTask(TaskSpec task) {
    RayLog.core.debug("Submitting task: {}", task);

    ByteBuffer info = taskSpec2Info(task);
    byte[] cursorId = null;
    if (!task.actorId.isNil()) {
      cursorId = task.cursorId.getBytes();
    }

    nativeSubmitTask(client, cursorId, info, info.position(), info.remaining());
  }

  @Override
  public TaskSpec getTask() {
    byte[] bytes = nativeGetTask(client);
    assert (null != bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return taskInfo2Spec(bb);
  }

  @Override
  public void reconstructObjects(List<UniqueId> objectIds, boolean fetchOnly) {
    if (RayLog.core.isInfoEnabled()) {
      RayLog.core.info("Reconstructing objects for task {}, object IDs are {}",
          UniqueIdHelper.computeTaskId(objectIds.get(0)), objectIds);
    }
    nativeReconstructObjects(client, getIdBytes(objectIds), fetchOnly);
  }

  @Override
  public UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex) {
    byte[] bytes = nativeGenerateTaskId(driverId.getBytes(), parentTaskId.getBytes(), taskIndex);
    return new UniqueId(bytes);
  }

  @Override
  public void notifyUnblocked() {
    nativeNotifyUnblocked(client);
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    byte[][] objectIdsArray = getIdBytes(objectIds);
    nativeFreePlasmaObjects(client, objectIdsArray, localOnly);
  }

  public static TaskSpec taskInfo2Spec(ByteBuffer bb) {
    bb.order(ByteOrder.LITTLE_ENDIAN);
    TaskInfo info = TaskInfo.getRootAsTaskInfo(bb);

    TaskSpec spec = new TaskSpec();
    spec.driverId = UniqueId.fromByteBuffer(info.driverIdAsByteBuffer());
    spec.taskId = UniqueId.fromByteBuffer(info.taskIdAsByteBuffer());
    spec.parentTaskId = UniqueId.fromByteBuffer(info.parentTaskIdAsByteBuffer());
    spec.parentCounter = info.parentCounter();
    spec.actorId = UniqueId.fromByteBuffer(info.actorIdAsByteBuffer());
    spec.actorCounter = info.actorCounter();
    spec.createActorId = UniqueId.fromByteBuffer(info.actorCreationIdAsByteBuffer());

    spec.functionId = UniqueId.fromByteBuffer(info.functionIdAsByteBuffer());

    List<FunctionArg> args = new ArrayList<>();
    for (int i = 0; i < info.argsLength(); i++) {
      UniqueId id = null;
      byte[] data = null;
      Arg sarg = info.args(i);

      int idCount = sarg.objectIdsLength();
      if (idCount > 0) {
        ByteBuffer lbb = sarg.objectIdAsByteBuffer(0);
        assert (lbb != null && lbb.remaining() > 0);
        id = UniqueId.fromByteBuffer(lbb);
      }

      ByteBuffer lbb = sarg.dataAsByteBuffer();
      if (lbb != null && lbb.remaining() > 0) {
        // TODO: how to avoid memory copy
        data = new byte[lbb.remaining()];
        lbb.get(data);
      }

      args.add(new FunctionArg(id, data));
    }
    spec.args = args.toArray(new FunctionArg[0]);

    List<UniqueId> rids = new ArrayList<>();
    for (int i = 0; i < info.returnsLength(); i++) {
      ByteBuffer lbb = info.returnsAsByteBuffer(i);
      assert (lbb != null && lbb.remaining() > 0);
      rids.add(UniqueId.fromByteBuffer(lbb));
    }
    spec.returnIds = rids.toArray(new UniqueId[0]);

    return spec;
  }

  public static ByteBuffer taskSpec2Info(TaskSpec task) {
    ByteBuffer bb = _taskBuffer.get();
    bb.clear();

    FlatBufferBuilder fbb = new FlatBufferBuilder(bb);

    final int driverIdOffset = fbb.createString(task.driverId.toByteBuffer());
    final int taskIdOffset = fbb.createString(task.taskId.toByteBuffer());
    final int parentTaskIdOffset = fbb.createString(task.parentTaskId.toByteBuffer());
    final int parentCounter = task.parentCounter;
    final int actorCreateIdOffset = fbb.createString(task.createActorId.toByteBuffer());
    final int actorCreateDummyIdOffset = fbb.createString(UniqueId.NIL.toByteBuffer());
    final int actorIdOffset = fbb.createString(task.actorId.toByteBuffer());
    final int actorHandleIdOffset = fbb.createString(task.actorHandleId.toByteBuffer());
    final int actorCounter = task.actorCounter;
    final int functionIdOffset = fbb.createString(task.functionId.toByteBuffer());

    // serialize args
    int[] argsOffsets = new int[task.args.length];
    for (int i = 0; i < argsOffsets.length; i++) {

      int objectIdOffset = 0;
      int dataOffset = 0;
      if (task.args[i].id != null) {
        int[] idOffsets = new int[] {
            fbb.createString(task.args[i].id.toByteBuffer())
        };
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

    // serialize returns
    int returnCount = task.returnIds.length;
    int[] returnsOffsets = new int[returnCount];
    for (int k = 0; k < returnCount; k++) {
      returnsOffsets[k] = fbb.createString(task.returnIds[k].toByteBuffer());
    }
    int returnsOffset = fbb.createVectorOfTables(returnsOffsets);

    // serialize required resources
    // The required_resources vector indicates the quantities of the different
    // resources required by this task. The index in this vector corresponds to
    // the resource type defined in the ResourceIndex enum. For example,
    int[] requiredResourcesOffsets = new int[task.resources.size()];
    int i = 0;
    for (Map.Entry<String, Double> entry : task.resources.entrySet()) {
      int keyOffset = fbb.createString(ByteBuffer.wrap(entry.getKey().getBytes()));
      requiredResourcesOffsets[i] =
          ResourcePair.createResourcePair(fbb, keyOffset, entry.getValue());
      i++;
    }

    int requiredResourcesOffset = fbb.createVectorOfTables(requiredResourcesOffsets);

    int root = TaskInfo.createTaskInfo(
        fbb, driverIdOffset, taskIdOffset,
        parentTaskIdOffset, parentCounter,
        actorCreateIdOffset, actorCreateDummyIdOffset,
        actorIdOffset, actorHandleIdOffset, actorCounter,
        false, functionIdOffset,
        argsOffset, returnsOffset, requiredResourcesOffset, TaskLanguage.JAVA);

    fbb.finish(root);
    ByteBuffer buffer = fbb.dataBuffer();

    if (buffer.remaining() > AbstractRayRuntime.getParams().max_submit_task_buffer_size_bytes) {
      RayLog.core.error(
          "Allocated buffer is not enough to transfer the task specification: " + AbstractRayRuntime
              .getParams().max_submit_task_buffer_size_bytes + " vs " + buffer.remaining());
      assert (false);
    }

    return buffer;
  }

  private static byte[][] getIdBytes(List<UniqueId> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
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
  /// 1) pushd $Dir/java/runtime-native/target/classes
  /// 2) javah -classpath .:$Dir/java/runtime-common/target/classes/:$Dir/java/api/target/classes/
  ///    org.ray.spi.impl.DefaultLocalSchedulerClient
  /// 3) clang-format -i org_ray_spi_impl_DefaultLocalSchedulerClient.h
  /// 4) cp org_ray_spi_impl_DefaultLocalSchedulerClient.h $Dir/src/local_scheduler/lib/java/
  /// 5) vim $Dir/src/local_scheduler/lib/java/org_ray_spi_impl_DefaultLocalSchedulerClient.cc
  /// 6) popd

  private static native long nativeInit(String localSchedulerSocket, byte[] workerId,
      boolean isWorker, byte[] driverTaskId);

  private static native void nativeSubmitTask(long client, byte[] cursorId, ByteBuffer taskBuff,
      int pos, int taskSize);

  // return TaskInfo (in FlatBuffer)
  private static native byte[] nativeGetTask(long client);

  private static native void nativeDestroy(long client);

  private static native void nativeReconstructObjects(long client, byte[][] objectIds,
      boolean fetchOnly);

  private static native void nativeNotifyUnblocked(long client);

  private static native void nativePutObject(long client, byte[] taskId, byte[] objectId);

  private static native boolean[] nativeWaitObject(long conn, byte[][] objectIds,
      int numReturns, int timeout, boolean waitLocal);

  private static native byte[] nativeGenerateTaskId(byte[] driverId, byte[] parentTaskId,
      int taskIndex);

  private static native void nativeFreePlasmaObjects(long conn, byte[][] objectIds,
      boolean localOnly);

}
