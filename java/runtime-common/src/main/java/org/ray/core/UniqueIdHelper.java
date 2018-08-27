package org.ray.core;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.ray.api.UniqueID;


//
// see src/common/common.h for UniqueID layout
//
public class UniqueIdHelper {

  public static UniqueID taskComputeReturnId(UniqueID uid, int returnIndex) {
    return objectIdFromTaskId(uid, returnIndex);
  }

  private static UniqueID objectIdFromTaskId(UniqueID taskId, int index) {
    byte[] objId = new byte[UniqueID.LENGTH];
    System.arraycopy(taskId.getBytes(), 0, objId, 0, UniqueID.LENGTH);
    byte[] indexBytes = ByteBuffer.allocate(4).putInt(index).array();
    objId[0] = indexBytes[3];
    objId[1] = indexBytes[2];
    objId[2] = indexBytes[1];
    objId[3] = indexBytes[0];

    UniqueID oid = new UniqueID(objId);
    return oid;
  }

  public static UniqueID taskComputePutId(UniqueID uid, int putIndex) {
    return objectIdFromTaskId(uid, -1 * putIndex);
  }

  public static UniqueID taskIdFromObjectId(UniqueID objectId) {
    byte[] taskId = new byte[20];
    System.arraycopy(objectId.getBytes(), 0, taskId, 0, UniqueID.LENGTH);
    Arrays.fill(taskId, 0, 3, (byte) 0);

    UniqueID retId = new UniqueID(taskId);
    return retId;
  }

}
