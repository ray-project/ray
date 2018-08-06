package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.spi.LocalSchedulerLink;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.TaskSpec;
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
                                     UniqueID actorId, boolean isWorker, UniqueID driverId,
                                     long numGpus, boolean useRaylet) {
    client = _init(schedulerSockName, clientId.getBytes(), actorId.getBytes(), isWorker,
        driverId.getBytes(), numGpus, useRaylet);
    this.useRaylet = useRaylet;
  }

  private static native long _init(String localSchedulerSocket, byte[] workerId,
                                   byte[] actorId, boolean isWorker, byte[] driverTaskId,
                                   long numGpus, boolean useRaylet);

  private static native byte[] _computePutId(long client, byte[] taskId, int putIndex);

  private static native void _task_done(long client);

  private static native boolean[] _waitObject(long conn, byte[][] objectIds, 
       int numReturns, int timeout, boolean waitLocal);

  @Override
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    assert (useRaylet == true);

    boolean[] readys = _waitObject(client, objectIds, numReturns, timeoutMs, false);
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
    ByteBuffer info = taskSpec2Info(task);
    byte[] a = null;
    if (!task.actorId.isNil()) {
      a = task.cursorId.getBytes();
    }

    _submitTask(client, a, info, info.position(), info.remaining(), useRaylet);
  }

  @Override
  public TaskSpec getTaskTodo() {
    byte[] bytes = _getTaskTodo(client, useRaylet);
    assert (null != bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return taskInfo2Spec(bb);
  }

  @Override
  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {
    _put_object(client, taskId.getBytes(), objectId.getBytes());
  }

  @Override
  public void reconstructObject(UniqueID objectId, boolean fetchOnly) {
    List<UniqueID> objects = new ArrayList<>();
    objects.add(objectId);
    _reconstruct_objects(client, getIdBytes(objects), fetchOnly);
  }

  @Override
  public void reconstructObjects(List<UniqueID> objectIds, boolean fetchOnly) {
    RayLog.core.info("reconstruct objects {}", objectIds);
    _reconstruct_objects(client, getIdBytes(objectIds), fetchOnly);
  }

  @Override
  public void notifyUnblocked() {
    _notify_unblocked(client);
  }

  private static native void _notify_unblocked(long client);

  private static native void _reconstruct_objects(long client, byte[][] objectIds,
                                                  boolean fetchOnly);

  private static native void _put_object(long client, byte[] taskId, byte[] objectId);

  // return TaskInfo (in FlatBuffer)
  private static native byte[] _getTaskTodo(long client, boolean useRaylet);

  public static TaskSpec taskInfo2Spec(ByteBuffer bb) {
    bb.order(ByteOrder.LITTLE_ENDIAN);
    TaskInfo info = TaskInfo.getRootAsTaskInfo(bb);

    TaskSpec spec = new TaskSpec();
    spec.driverId = new UniqueID(info.driverIdAsByteBuffer());
    spec.taskId = new UniqueID(info.taskIdAsByteBuffer());
    spec.parentTaskId = new UniqueID(info.parentTaskIdAsByteBuffer());
    spec.parentCounter = info.parentCounter();
    spec.actorId = new UniqueID(info.actorIdAsByteBuffer());
    spec.actorCounter = info.actorCounter();
    spec.createActorId = new UniqueID(info.actorCreationIdAsByteBuffer());

    spec.functionId = new UniqueID(info.functionIdAsByteBuffer());

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
          darg.ids.add(new UniqueID(lbb));
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
      rids.add(new UniqueID(lbb));
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
    final int actorCreateDummyIdOffset = fbb.createString(UniqueID.nil.toByteBuffer());
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

    int[]requiredResourcesOffsets = new int[1];
    for (int i = 0; i < requiredResourcesOffsets.length; i++) {
      int keyOffset = 0;
      keyOffset = fbb.createString(ByteBuffer.wrap("CPU".getBytes()));
      requiredResourcesOffsets[i] = ResourcePair.createResourcePair(fbb, keyOffset, 0.0);
    }
    int requiredResourcesOffset = fbb.createVectorOfTables(requiredResourcesOffsets);

    int root = TaskInfo.createTaskInfo(
        fbb, driverIdOffset, taskIdOffset,
        parentTaskIdOffset, parentCounter,
        actorCreateIdOffset, actorCreateDummyIdOffset,
        actorIdOffset, actorHandleIdOffset, actorCounter,
        false, functionIdOffset,
        argsOffset, returnsOffset, requiredResourcesOffset);

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

  // task -> TaskInfo (with FlatBuffer)
  protected static native void _submitTask(long client, byte[] cursorId, /*Direct*/ByteBuffer task,
                                         int pos, int sz, boolean useRaylet);

  private static byte[][] getIdBytes(List<UniqueID> objectIds) {
    int size = objectIds.size();
    byte[][] ids = new byte[size][];
    for (int i = 0; i < size; i++) {
      ids[i] = objectIds.get(i).getBytes();
    }
    return ids;
  }

  public void destroy() {
    _destroy(client);
  }

  private static native void _destroy(long client);
}
