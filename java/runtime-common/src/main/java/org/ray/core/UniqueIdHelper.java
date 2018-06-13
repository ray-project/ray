package org.ray.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
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
  private static final int batchPos = 0;
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

  public static Long getNextCreateThreadRandomSeed() {
    UniqueID currentTaskId = WorkerContext.currentTask().taskId;
    byte[] bytes;

    ByteBuffer lbuffer = longBuffer.get();
    // similar to task id generation (see nextTaskId below)
    if (!currentTaskId.isNil()) {
      ByteBuffer rbb = ByteBuffer.wrap(currentTaskId.getBytes());
      rbb.order(ByteOrder.LITTLE_ENDIAN);
      long cid = rbb.getLong(uniquenessPos);
      byte[] cbuffer = lbuffer.putLong(cid).array();
      bytes = MD5Digestor.digest(cbuffer, WorkerContext.nextCallIndex());
    } else {
      long cid = rand.get().nextLong();
      byte[] cbuffer = lbuffer.putLong(cid).array();
      bytes = MD5Digestor.digest(cbuffer, rand.get().nextLong());
    }
    lbuffer.clear();

    lbuffer.put(bytes, 0, Long.SIZE / Byte.SIZE);
    long r = lbuffer.getLong();
    lbuffer.clear();
    return r;
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
    UniqueID oid = newZero();
    ByteBuffer rbb = ByteBuffer.wrap(taskId.getBytes());
    rbb.order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer wbb = ByteBuffer.wrap(oid.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setBatch(wbb, getBatch(rbb));
    setUniqueness(wbb, getUniqueness(rbb));
    setType(wbb, Type.OBJECT);
    setHasMultipleReturn(wbb, hasMultipleReturn ? 1 : 0);
    setIsReturn(wbb, isReturn ? 1 : 0);
    setWithinTaskIndex(wbb, index);
    return oid;
  }

  private static UniqueID newZero() {
    byte[] b = new byte[UniqueID.LENGTH];
    Arrays.fill(b, (byte) 0);
    return new UniqueID(b);
  }

  private static void setBatch(ByteBuffer bb, long batchId) {
    bb.putLong(batchPos, batchId);
  }

  private static long getBatch(ByteBuffer bb) {
    return bb.getLong(batchPos);
  }

  private static void setUniqueness(ByteBuffer bb, long uniqueness) {
    bb.putLong(uniquenessPos, uniqueness);
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
    return objectIdFromTaskId(uid, false, false, putIndex);
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
    UniqueID taskId = newZero();
    ByteBuffer rbb = ByteBuffer.wrap(objectId.getBytes());
    rbb.order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer wbb = ByteBuffer.wrap(taskId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setBatch(wbb, getBatch(rbb));
    setUniqueness(wbb, getUniqueness(rbb));
    setType(wbb, Type.TASK);
    return taskId;
  }

  public static UniqueID nextTaskId(long batchId) {
    UniqueID taskId = newZero();
    ByteBuffer wbb = ByteBuffer.wrap(taskId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setType(wbb, Type.TASK);

    UniqueID currentTaskId = WorkerContext.currentTask().taskId;
    ByteBuffer rbb = ByteBuffer.wrap(currentTaskId.getBytes());
    rbb.order(ByteOrder.LITTLE_ENDIAN);

    // setup batch id
    if (batchId == -1) {
      setBatch(wbb, getBatch(rbb));
    } else {
      setBatch(wbb, batchId);
    }

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

  public static boolean isLambdaFunction(UniqueID functionId) {
    ByteBuffer wbb = ByteBuffer.wrap(functionId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    return wbb.getLong() == 0xffffffffffffffffL;
  }

  public static void markCreateActorStage1Function(UniqueID functionId) {
    ByteBuffer wbb = ByteBuffer.wrap(functionId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    setUniqueness(wbb, 1);
  }

  // WARNING: see hack in MethodId.java which must be aligned with here
  public static boolean isNonLambdaCreateActorStage1Function(UniqueID functionId) {
    ByteBuffer wbb = ByteBuffer.wrap(functionId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    return getUniqueness(wbb) == 1;
  }

  public static boolean isNonLambdaCommonFunction(UniqueID functionId) {
    ByteBuffer wbb = ByteBuffer.wrap(functionId.getBytes());
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    return getUniqueness(wbb) == 0;
  }

  public enum Type {
    OBJECT,
    TASK,
    ACTOR,
  }
}
