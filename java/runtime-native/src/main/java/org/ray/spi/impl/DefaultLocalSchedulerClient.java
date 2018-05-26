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
 * JNI-based local scheduler link provider
 */
public class DefaultLocalSchedulerClient implements LocalSchedulerLink {

  public DefaultLocalSchedulerClient(String schedulerSockName, UniqueID clientId, UniqueID actorId,
      boolean isWorker, long numGpus) {
    _client = _init(schedulerSockName, clientId.getBytes(), actorId.getBytes(), isWorker,
        numGpus);
  }


  @Override
  public void submitTask(TaskSpec task) {
    ByteBuffer info = TaskSpec2Info(task);
    byte[] a = null;
    if(!task.actorId.isNil()) {
      a = task.cursorId.getBytes();
    }
    _submitTask(_client, a, info, info.position(), info.remaining());
  }

  @Override
  public TaskSpec getTaskTodo() {
    byte[] bytes = _getTaskTodo(_client);
    assert (null != bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return TaskInfo2Spec(bb);
  }

  public void destroy() {
    _destroy(_client);
  }

  @Override
  public void notifyUnblocked() {
    _notify_unblocked(_client);
  }

  @Override
  public void reconstructObject(UniqueID objectId) {
    _reconstruct_object(_client, objectId.getBytes());
  }

  @Override
  public void markTaskPutDependency(UniqueID taskId, UniqueID objectId) {
    _put_object(_client, taskId.getBytes(), objectId.getBytes());
  }

  private long _client = 0;

  private static ThreadLocal<ByteBuffer> _taskBuffer = ThreadLocal.withInitial(() -> {
    ByteBuffer bb = ByteBuffer
        .allocateDirect(RayRuntime.getParams().max_submit_task_buffer_size_bytes);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return bb;
  });

  public static ByteBuffer TaskSpec2Info(TaskSpec task) {
    ByteBuffer bb = _taskBuffer.get();
    bb.clear();

    FlatBufferBuilder fbb = new FlatBufferBuilder(bb);

    int driver_idOffset = fbb.createString(task.driverId.ToByteBuffer());
    int task_idOffset = fbb.createString(task.taskId.ToByteBuffer());
    int parent_task_idOffset = fbb.createString(task.parentTaskId.ToByteBuffer());
    int parent_counter = task.parentCounter;
    int actorCreate_idOffset = fbb.createString(task.createActorId.ToByteBuffer());
    int actorCreateDummy_idOffset = fbb.createString(UniqueID.nil.ToByteBuffer());
    int actor_idOffset = fbb.createString(task.actorId.ToByteBuffer());
    int actor_handle_idOffset = fbb.createString(task.actorHandleId.ToByteBuffer());
    int actor_counter = task.actorCounter;
    int function_idOffset = fbb.createString(task.functionId.ToByteBuffer());

    // serialize args
    int[] argsOffsets = new int[task.args.length];
    for (int i = 0; i < argsOffsets.length; i++) {

      int object_idOffset = 0;
      int dataOffset = 0;
      if (task.args[i].ids != null) {
        int id_count = task.args[i].ids.size();
        int[] idOffsets = new int[id_count];
        for (int k = 0; k < id_count; k++) {
          idOffsets[k] = fbb.createString(task.args[i].ids.get(k).ToByteBuffer());
        }
        object_idOffset = fbb.createVectorOfTables(idOffsets);
      }
      if (task.args[i].data != null) {
        dataOffset = fbb.createString(ByteBuffer.wrap(task.args[i].data));
      }

      argsOffsets[i] = Arg.createArg(fbb, object_idOffset, dataOffset);
    }
    int argsOffset = fbb.createVectorOfTables(argsOffsets);

    // serialize returns
    int return_count = task.returnIds.length;
    int[] returnsOffsets = new int[return_count];
    for (int k = 0; k < return_count; k++) {
      returnsOffsets[k] = fbb.createString(task.returnIds[k].ToByteBuffer());
    }
    int returnsOffset = fbb.createVectorOfTables(returnsOffsets);

    // serialize required resources
    // The required_resources vector indicates the quantities of the different
    // resources required by this task. The index in this vector corresponds to
    // the resource type defined in the ResourceIndex enum. For example,

    int[] required_resourcesOffsets = new int[1];
    for (int i = 0; i < required_resourcesOffsets.length; i++) {
        int keyOffset = 0;
        keyOffset = fbb.createString(ByteBuffer.wrap("CPU".getBytes()));
        required_resourcesOffsets[i] = ResourcePair.createResourcePair(fbb,keyOffset,0.0);
    }
    int requiredResourcesOffset = fbb.createVectorOfTables(required_resourcesOffsets);

    int root = TaskInfo.createTaskInfo(
        fbb, driver_idOffset, task_idOffset,
        parent_task_idOffset, parent_counter,
        actorCreate_idOffset, actorCreateDummy_idOffset,
        actor_idOffset, actor_handle_idOffset, actor_counter,
        false, function_idOffset,
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

  public static TaskSpec TaskInfo2Spec(ByteBuffer bb) {
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

      int id_count = sarg.objectIdsLength();
      if (id_count > 0) {
        darg.ids = new ArrayList<>();
        for (int j = 0; j < id_count; j++) {
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

  native private static long _init(String localSchedulerSocket, byte[] workerId, byte[] actorId,
      boolean isWorker, long numGpus);

  // task -> TaskInfo (with FlatBuffer)
  native private static void _submitTask(long client, byte[] cursorId, /*Direct*/ByteBuffer task, int pos, int sz);

  // return TaskInfo (in FlatBuffer)
  native private static byte[] _getTaskTodo(long client);

  native private static byte[] _computePutId(long client, byte[] taskId, int putIndex);

  native private static void _destroy(long client);

  native private static void _task_done(long client);

  native private static void _reconstruct_object(long client, byte[] objectId);

  native private static void _notify_unblocked(long client);

  native private static void _put_object(long client, byte[] taskId, byte[] objectId);
}
