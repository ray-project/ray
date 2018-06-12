package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.spi.impl.DefaultLocalSchedulerClient;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.TaskSpec;
import org.ray.util.logger.RayLog;

public class DefaultRayletClient extends DefaultLocalSchedulerClient implements ObjectStoreLink {

  public DefaultRayletClient(String schedulerSockName, UniqueID clientId, UniqueID actorId,
      boolean isWorker, long numGpus) {
    super(schedulerSockName, clientId, actorId, isWorker, numGpus);
  }

  @Override
  public void submitTask(TaskSpec task) {
    ByteBuffer info = taskSpec2Info(task);
    byte[] a = null;
    if(!task.actorId.isNil()) {
      a = task.cursorId.getBytes();
    }
    _submitTask(client, a, info, info.position(), info.remaining(), true);
  }

  // interface methods --------------------

  @Override
  public void put(byte[] objectId, byte[] value, byte[] metadata) {
    /*ByteBuffer buf = null;
    try {
      buf = PlasmaClientJNI.create(conn, objectId, value.length, metadata);
    } catch (Exception e) {
      System.err.println("ObjectId " + objectId + " error at PlasmaClient put");
      e.printStackTrace();
    }
    if (buf == null) {
      return;
    }

    buf.put(value);
    PlasmaClientJNI.seal(conn, objectId);
    PlasmaClientJNI.release(conn, objectId);*/
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    /* ByteBuffer[][] bufs = PlasmaClientJNI.get(conn, objectIds, timeoutMs);
    assert bufs.length == objectIds.length;

    List<byte[]> ret = new ArrayList<>();
    for (int i = 0; i < bufs.length; i++) {
      ByteBuffer buf = bufs[i][isMetadata ? 1 : 0];
      if (buf == null) {
        ret.add(null);
      } else {
        byte[] bb = new byte[buf.remaining()];
        buf.get(bb);
        ret.add(bb);
      }
    }
    return ret; */
    return null;
  }

  @Override
  public List<byte[]> wait(byte[][] objectIds, int timeoutMs, int numReturns) {
    byte[][] readys = _wait(client, objectIds, timeoutMs, numReturns);

    List<byte[]> ret = new ArrayList<>();
    for (byte[] ready : readys) {
      for (byte[] id : objectIds) {
        if (Arrays.equals(ready, id)) {
          ret.add(id);
          break;
        }
      }
    }

    assert (ret.size() == readys.length);
    return ret; 
  }

  @Override
  public byte[] hash(byte[] objectId) {
    // return PlasmaClientJNI.hash(conn, objectId);
    return null;
  }

  @Override
  public void fetch(byte[][] objectIds) {
    // PlasmaClientJNI.fetch(conn, objectIds);
    return;
  }

  @Override
  public long evict(long numBytes) {
    // return PlasmaClientJNI.evict(conn, numBytes);
    return 0;
  }

  // wrapper methods --------------------

  /**
   * Seal the buffer in the PlasmaStore for a particular object ID.
   * Once a buffer has been sealed, the buffer is immutable and can only be accessed through get.
   *
   * @param objectId used to identify an object.
   */
  public void seal(byte[] objectId) {
    // PlasmaClientJNI.seal(conn, objectId);
  }

  /**
   * Notify Plasma that the object is no longer needed.
   *
   * @param objectId used to identify an object.
   */
  public void release(byte[] objectId) {
    // PlasmaClientJNI.release(conn, objectId);
  }

  /**
   * Check if the object is present and has been sealed in the PlasmaStore.
   *
   * @param objectId used to identify an object.
   */
  @Override
  public boolean contains(byte[] objectId) {
    // return PlasmaClientJNI.contains(conn, objectId);
    return false;
  }

  native private static byte[][] _wait(long conn, byte[][] object_ids, int timeout_ms,
      int num_returns);
}