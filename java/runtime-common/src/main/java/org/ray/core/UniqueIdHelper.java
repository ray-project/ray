package org.ray.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.ray.api.UniqueID;


//
// see src/common/common.h for UniqueID layout
//
public class UniqueIdHelper {

  public static UniqueID computeReturnId(UniqueID taskId, int returnIndex) {
    return computeObjectId(taskId, returnIndex);
  }

  private static UniqueID computeObjectId(UniqueID taskId, int index) {
    byte[] objId = new byte[UniqueID.LENGTH];
    System.arraycopy(taskId.getBytes(),0, objId, 0, UniqueID.LENGTH);
    ByteBuffer wbb = ByteBuffer.wrap(objId);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(UniqueID.OBJECT_INDEX_POS, index);

    return new UniqueID(objId);
  }

  public static UniqueID computePutId(UniqueID uid, int putIndex) {
    // We multiply putIndex by -1 to distinguish from returnIndex.
    return computeObjectId(uid, -1 * putIndex);
  }

  public static UniqueID computeTaskId(UniqueID objectId) {
    byte[] taskId = new byte[UniqueID.LENGTH];
    System.arraycopy(objectId.getBytes(), 0, taskId, 0, UniqueID.LENGTH);
    Arrays.fill(taskId, UniqueID.OBJECT_INDEX_POS,
        UniqueID.OBJECT_INDEX_POS + UniqueID.OBJECT_INDEX_LENGTH, (byte) 0);

    return new UniqueID(taskId);
  }

}
