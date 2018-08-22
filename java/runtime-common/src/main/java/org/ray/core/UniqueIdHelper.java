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

  private static final ThreadLocal<ByteBuffer> longBuffer = ThreadLocal
      .withInitial(() -> ByteBuffer.allocate(Long.SIZE / Byte.SIZE));
  private static final ThreadLocal<Random> rand = ThreadLocal.withInitial(Random::new);
  private static final ThreadLocal<Long> randSeed = new ThreadLocal<>();
  private static final int uniquenessPos = Long.SIZE / Byte.SIZE;
  private static final int typePos = 2 * Long.SIZE / Byte.SIZE;
  private static final BitField typeField = new BitField(0x7);
  private static final int testPos = 2 * Long.SIZE / Byte.SIZE;
  private static final BitField testField = new BitField(0x1 << 3);
  private static final int unionPos = 2 * Long.SIZE / Byte.SIZE;
  private static final BitField multipleReturnField = new BitField(0x1 << 8);
  private static final BitField isReturnIdField = new BitField(0x1 << 9);
  private static final BitField withinTaskIndexField = new BitField(0xFFFFFC00);

  public static void setThreadRandomSeed(long seed) {
    if (randSeed.get() != null) {
      RayLog.core.error("Thread random seed is already set to " + randSeed.get()
          + " and now to be overwritten to " + seed);
      throw new RuntimeException("Thread random seed is already set to " + randSeed.get()
          + " and now to be overwritten to " + seed);
    }

    RayLog.core.debug("Thread random seed is set to " + seed);
    randSeed.set(seed);
    rand.get().setSeed(seed);
  }

  private static Type getType(ByteBuffer bb) {
    byte v = bb.get(typePos);
    return Type.values()[typeField.getValue(v)];
  }

  private static boolean getIsTest(ByteBuffer bb) {
    byte v = bb.get(testPos);
    return testField.getValue(v) == 1;
  }

  private static int getIsReturn(ByteBuffer bb) {
    int v = bb.getInt(unionPos);
    return isReturnIdField.getValue(v);
  }

  private static int getWithinTaskIndex(ByteBuffer bb) {
    int v = bb.getInt(unionPos);
    return withinTaskIndexField.getValue(v);
  }

  public static void setTest(UniqueID id, boolean isTest) {
    ByteBuffer bb = ByteBuffer.wrap(id.getBytes());
    setIsTest(bb, isTest);
  }

  private static void setIsTest(ByteBuffer bb, boolean isTest) {
    byte v = bb.get(testPos);
    v = (byte) testField.setValue(v, isTest ? 1 : 0);
    bb.put(testPos, v);
  }

  public static long getUniqueness(UniqueID id) {
    ByteBuffer bb = ByteBuffer.wrap(id.getBytes());
    bb.order(ByteOrder.LITTLE_ENDIAN);
    return getUniqueness(bb);
  }

  private static long getUniqueness(ByteBuffer bb) {
    return bb.getLong(uniquenessPos);
  }

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

  private static void setUniqueness(ByteBuffer bb, byte[] uniqueness) {
    for (int i = 0; i < Long.SIZE / Byte.SIZE; ++i) {
      bb.put(uniquenessPos + i, uniqueness[i]);
    }
  }

  private static void setType(ByteBuffer bb, Type type) {
    byte v = bb.get(typePos);
    v = (byte) typeField.setValue(v, type.ordinal());
    bb.put(typePos, v);
  }

  private static void setHasMultipleReturn(ByteBuffer bb, int hasMultipleReturnOrNot) {
    int v = bb.getInt(unionPos);
    v = multipleReturnField.setValue(v, hasMultipleReturnOrNot);
    bb.putInt(unionPos, v);
  }

  private static void setIsReturn(ByteBuffer bb, int isReturn) {
    int v = bb.getInt(unionPos);
    v = isReturnIdField.setValue(v, isReturn);
    bb.putInt(unionPos, v);
  }

  private static void setWithinTaskIndex(ByteBuffer bb, int index) {
    int v = bb.getInt(unionPos);
    v = withinTaskIndexField.setValue(v, index);
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

  public static UniqueID nextTaskId(long batchId) {
    UniqueID taskId = newZero();
    ByteBuffer wbb = ByteBuffer.wrap(taskId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setType(wbb, Type.TASK);

    UniqueID currentTaskId = WorkerContext.currentTask().taskId;
    ByteBuffer rbb = ByteBuffer.wrap(currentTaskId.getBytes());
    rbb.order(ByteOrder.LITTLE_ENDIAN);

    // setup unique id (task id)
    byte[] idBytes;

    ByteBuffer lbuffer = longBuffer.get();
    // if inside a task
    if (!currentTaskId.isNil()) {
      long cid = rbb.getLong(uniquenessPos);
      byte[] cbuffer = lbuffer.putLong(cid).array();
      idBytes = MD5Digestor.digest(cbuffer, WorkerContext.nextCallIndex());

      // if not
    } else {
      long cid = rand.get().nextLong();
      byte[] cbuffer = lbuffer.putLong(cid).array();
      idBytes = MD5Digestor.digest(cbuffer, rand.get().nextLong());
    }
    setUniqueness(wbb, idBytes);
    lbuffer.clear();

    return taskId;
  }

  public enum Type {
    OBJECT,
    TASK,
    ACTOR,
  }
}