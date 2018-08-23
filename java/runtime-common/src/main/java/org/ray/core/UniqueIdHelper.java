package org.ray.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import org.apache.commons.lang3.BitField;
import org.ray.api.UniqueID;
import org.ray.util.MD5Digestor;
import org.ray.util.logger.RayLog;


//
// see src/common/common.h for UniqueID layout
//
public class UniqueIdHelper {
  private static final int unionPos = 2 * Long.SIZE / Byte.SIZE;
  private static final BitField multipleReturnField = new BitField(0x1 << 8);


  public static UniqueID taskComputeReturnId(
      UniqueID uid,
      int returnIndex,
      boolean hasMultipleReturn
  ) {
    return objectIdFromTaskId(uid, true, hasMultipleReturn, returnIndex);
  }

  private static UniqueID objectIdFromTaskId(UniqueID taskId,
                                             boolean isReturn,
                                             boolean hasMultipleReturn,
                                             int index
  ) {
    byte[] objId = new byte[20];
    System.arraycopy(taskId.getBytes(), 0, objId, 0, 20);
    byte[] indexBytes = ByteBuffer.allocate(4).putInt(index).array();
    objId[0] = indexBytes[3];
    objId[1] = indexBytes[2];
    objId[2] = indexBytes[1];
    objId[3] = indexBytes[0];

    UniqueID oid = new UniqueID(objId);

    ByteBuffer wbb = ByteBuffer.wrap(oid.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setHasMultipleReturn(wbb, hasMultipleReturn ? 1 : 0);

    return oid;
  }

  private static UniqueID newZero() {
    byte[] b = new byte[UniqueID.LENGTH];
    Arrays.fill(b, (byte) 0);
    return new UniqueID(b);
  }

  private static void setHasMultipleReturn(ByteBuffer bb, int hasMultipleReturnOrNot) {
    int v = bb.getInt(unionPos);
    v = multipleReturnField.setValue(v, hasMultipleReturnOrNot);
    bb.putInt(unionPos, v);
  }

  public static UniqueID taskComputePutId(UniqueID uid, int putIndex) {
    return objectIdFromTaskId(uid, false, false, -1 * putIndex);
  }

  public static boolean hasMultipleReturnOrNotFromReturnObjectId(UniqueID returnId) {
    ByteBuffer bb = ByteBuffer.wrap(returnId.getBytes());
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return getHasMultipleReturn(bb) != 0;
  }

  private static int getHasMultipleReturn(ByteBuffer bb) {
    int v = bb.getInt(unionPos);
    return multipleReturnField.getValue(v);
  }

  public static UniqueID taskIdFromObjectId(UniqueID objectId) {
    byte[] taskId = new byte[20];
    System.arraycopy(objectId.getBytes(), 0, taskId, 0, 20);
    taskId[0] = 0;
    taskId[1] = 0;
    taskId[2] = 0;
    taskId[3] = 0;

    UniqueID retId = new UniqueID(taskId);
    return retId;
  }

}
