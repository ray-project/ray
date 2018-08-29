package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
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
        .allocateDirect(RayRuntime.getParams().max_submit_task_buffer_size_bytes);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return bb;
  });
  private long client = 0;
  boolean useRaylet = false;

  public DefaultLocalSchedulerClient(String schedulerSockName, UniqueID clientId,
      boolean isWorker, UniqueID driverId, boolean useRaylet) {
    client = nativeInit(schedulerSockName, clientId.getBytes(),
        isWorker, driverId.getBytes(), useRaylet);
    this.useRaylet = useRaylet;
  }

  @Override
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    assert (useRaylet == true);

    boolean[] readys = nativeWaitObject(client, objectIds, numReturns, timeoutMs, false);
    assert (readys.length == objectIds.length);

    List<byte[]> ret = new ArrayList<>();
    for (int i = 0; i < readys.length; i++) {
      if (readys[i]) {
        ret.add(objectIds[i]);
      }
    }

    return ret;
  }

  @Override
  public void submitTask(TaskSpec task) {
    // We don't support resources management in non raylet mode.
    if (!useRaylet) {
      task.resources.clear();
      task.resources.put(ResourceUtil.CPU_LITERAL, 0.0);
    } else {
      if (!task.resources.containsKey(ResourceUtil.CPU_LITERAL)) {
        task.resources.put(ResourceUtil.CPU_LITERAL, 0.0);
      }

      if (!task.resources.containsKey(ResourceUtil.GPU_LITERAL)) {
        task.resources.put(ResourceUtil.GPU_LITERAL, 0.0);
      }
    }

    ByteBuffer info = taskSpec2Info(task);
    byte[] a = null;
    if (!task.actorId.isNil()) {
      a = task.cursorId.getBytes();
    }

    nativeSubmitTask(client, a, info, info.position(), info.remaining(), useRaylet);
  }

  @Override
  public TaskSpec getTask() {
    byte[] bytes = nativeGetTask(client, useRaylet);
    assert (null != bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return taskInfo2Spec(bb);
  }

  @Override
  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {
    nativePutObject(client, taskId.getBytes(), objectId.getBytes());
  }

  @Override
  public void reconstructObject(UniqueID objectId, boolean fetchOnly) {
    List<UniqueID> objects = new ArrayList<>();
    objects.add(objectId);
    nativeReconstructObjects(client, getIdBytes(objects), fetchOnly);
  }

  @Override
  public void reconstructObjects(List<UniqueID> objectIds, boolean fetchOnly) {
    if (RayLog.core.isInfoEnabled()) {
      RayLog.core.info("Reconstructing objects for task {}, object IDs are {}",
          UniqueIdHelper.computeTaskId(objectIds.get(0)), objectIds);
    }
    nativeReconstructObjects(client, getIdBytes(objectIds), fetchOnly);
  }

  @Override
  public UniqueID generateTaskId(UniqueID driverId, UniqueID parentTaskId, int taskIndex) {
    byte[] bytes = nativeGenerateTaskId(driverId.getBytes(), parentTaskId.getBytes(), taskIndex);
    return new UniqueID(bytes);
  }

  @Override
  public void notifyUnblocked() {
    nativeNotifyUnblocked(client);
  }

  public static TaskSpec taskInfo2Spec(ByteBuffer bb) {
    bb.order(ByteOrder.LITTLE_ENDIAN);
    TaskInfo info = TaskInfo.getRootAsTaskInfo(bb);

    TaskSpec spec = new TaskSpec();
    spec.driverId = UniqueID.fromByteBuffer(info.driverIdAsByteBuffer());
    spec.taskId = UniqueID.fromByteBuffer(info.taskIdAsByteBuffer());
    spec.parentTaskId = UniqueID.fromByteBuffer(info.parentTaskIdAsByteBuffer());
    spec.parentCounter = info.parentCounter();
    spec.actorId = UniqueID.fromByteBuffer(info.actorIdAsByteBuffer());
    spec.actorCounter = info.actorCounter();
    spec.createActorId = UniqueID.fromByteBuffer(info.actorCreationIdAsByteBuffer());

    spec.functionId = UniqueID.fromByteBuffer(info.functionIdAsByteBuffer());

    List<FunctionArg> args = new ArrayList<>();
    for (int i = 0; i < info.argsLength(); i++) {
      FunctionArg darg = new FunctionArg();
      Arg sarg = info.args(i);

      int idCount = sarg.objectIdsLength();
      if (idCount > 0) {
        darg.ids = new ArrayList<>();
        for (int j = 0; j < idCount; j++) {
          ByteBuffer lbb = sarg.objectIdAsByteBuffer(j);
          assert (lbb != null && lbb.remaining() > 0);
          darg.ids.add(UniqueID.fromByteBuffer(lbb));
        }
      }

      ByteBuffer lbb = sarg.dataAsByteBuffer();
      if (lbb != null && lbb.remaining() > 0) {
        // TODO: how to avoid memory copy
        darg.data = new byte[lbb.remaining()];
        lbb.get(darg.data);
      }

      args.add(darg);
    }
    spec.args = args.toArray(new FunctionArg[0]);

    List<UniqueID> rids = new ArrayList<>();
    for (int i = 0; i < info.returnsLength(); i++) {
      ByteBuffer lbb = info.returnsAsByteBuffer(i);
      assert (lbb != null && lbb.remaining() > 0);
      rids.add(UniqueID.fromByteBuffer(lbb));
    }
    spec.returnIds = rids.toArray(new UniqueID[0]);

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
    final int actorCreateDummyIdOffset = fbb.createString(UniqueID.NIL.toByteBuffer());
    final int actorIdOffset = fbb.createString(task.actorId.toByteBuffer());
    final int actorHandleIdOffset = fbb.createString(task.actorHandleId.toByteBuffer());
    final int actorCounter = task.actorCounter;
    final int functionIdOffset = fbb.createString(task.functionId.toByteBuffer());

    // serialize args
    int[] argsOffsets = new int[task.args.length];
    for (int i = 0; i < argsOffsets.length; i++) {

      int objectIdOffset = 0;
      int dataOffset = 0;
      if (task.args[i].ids != null) {
        int idCount = task.args[i].ids.size();
        int[] idOffsets = new int[idCount];
        for (int k = 0; k < idCount; k++) {
          idOffsets[k] = fbb.createString(task.args[i].ids.get(k).toByteBuffer());
        }
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

    if (buffer.remaining() > RayRuntime.getParams().max_submit_task_buffer_size_bytes) {
      RayLog.core.error(
          "Allocated buffer is not enough to transfer the task specification: " + RayRuntime
              .getParams().max_submit_task_buffer_size_bytes + " vs " + buffer.remaining());
      assert (false);
    }

    return buffer;
  }

  private static byte[][] getIdBytes(List<UniqueID> objectIds) {
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


  // Native method declarations.

  private static native long nativeInit(String localSchedulerSocket, byte[] workerId,
      boolean isWorker, byte[] driverTaskId, boolean useRaylet);

  private static native void nativeSubmitTask(long client, byte[] cursorId, ByteBuffer taskBuff,
      int pos, int taskSize, boolean useRaylet);

  // return TaskInfo (in FlatBuffer)
  private static native byte[] nativeGetTask(long client, boolean useRaylet);

  private static native byte[] nativeComputePutId(long client, byte[] taskId, int putIndex);

  private static native void nativeDestroy(long client);

  private static native void nativeReconstructObjects(long client, byte[][] objectIds,
      boolean fetchOnly);

  private static native void nativeNotifyUnblocked(long client);

  private static native void nativePutObject(long client, byte[] taskId, byte[] objectId);

  private static native boolean[] nativeWaitObject(long conn, byte[][] objectIds,
      int numReturns, int timeout, boolean waitLocal);

  private static native byte[] nativeGenerateTaskId(byte[] driverId, byte[] parentTaskId,
      int taskIndex);

}
