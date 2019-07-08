package org.ray.api.test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import org.ray.api.id.ObjectId;
import org.ray.api.id.TaskId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.IdUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UniqueIdTest {

  @Test
  public void testConstructUniqueId() {
    // Test `fromHexString()`
    UniqueId id1 = UniqueId.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");
    Assert.assertEquals("00000000123456789abcdef123456789abcdef00", id1.toString());
    Assert.assertFalse(id1.isNil());

    try {
      UniqueId id2 = UniqueId.fromHexString("000000123456789ABCDEF123456789ABCDEF00");
      // This shouldn't be happened.
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    try {
      UniqueId id3 = UniqueId.fromHexString("GGGGGGGGGGGGG");
      // This shouldn't be happened.
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    // Test `fromByteBuffer()`
    byte[] bytes = DatatypeConverter.parseHexBinary("0123456789ABCDEF0123456789ABCDEF01234567");
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, 20);
    UniqueId id4 = UniqueId.fromByteBuffer(byteBuffer);
    Assert.assertTrue(Arrays.equals(bytes, id4.getBytes()));
    Assert.assertEquals("0123456789abcdef0123456789abcdef01234567", id4.toString());


    // Test `genNil()`
    UniqueId id6 = UniqueId.NIL;
    Assert.assertEquals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".toLowerCase(), id6.toString());
    Assert.assertTrue(id6.isNil());
  }

  @Test
  public void testComputeReturnId() {
    // Mock a taskId, and the lowest 4 bytes should be 0.
    TaskId taskId = TaskId.fromHexString("123456789ABCDEF123456789ABCDEF00");

    ObjectId returnId = IdUtil.computeReturnId(taskId, 1);
    Assert.assertEquals("123456789abcdef123456789abcdef0001000000", returnId.toString());

    returnId = IdUtil.computeReturnId(taskId, 0x01020304);
    Assert.assertEquals("123456789abcdef123456789abcdef0004030201", returnId.toString());
  }

  @Test
  public void testComputeTaskId() {
    ObjectId objId = ObjectId.fromHexString("123456789ABCDEF123456789ABCDEF0034421980");
    TaskId taskId = objId.getTaskId();

    Assert.assertEquals("123456789abcdef123456789abcdef00", taskId.toString());
  }

  @Test
  public void testComputePutId() {
    // Mock a taskId, the lowest 4 bytes should be 0.
    TaskId taskId = TaskId.fromHexString("123456789ABCDEF123456789ABCDEF00");

    ObjectId putId = IdUtil.computePutId(taskId, 1);
    Assert.assertEquals("123456789ABCDEF123456789ABCDEF00FFFFFFFF".toLowerCase(), putId.toString());

    putId = IdUtil.computePutId(taskId, 0x01020304);
    Assert.assertEquals("123456789ABCDEF123456789ABCDEF00FCFCFDFE".toLowerCase(), putId.toString());
  }

  @Test
  public void testUniqueIdsAndByteBufferInterConversion() {
    final int len = 5;
    UniqueId[] ids = new UniqueId[len];
    for (int i = 0; i < len; ++i) {
      ids[i] = UniqueId.randomId();
    }

    ByteBuffer temp = IdUtil.concatIds(ids);
    UniqueId[] res = IdUtil.getUniqueIdsFromByteBuffer(temp);

    for (int i = 0; i < len; ++i) {
      Assert.assertEquals(ids[i], res[i]);
    }
  }

  @Test
  void testMurmurHash() {
    UniqueId id = UniqueId.fromHexString("3131313131313131313132323232323232323232");
    long remainder = Long.remainderUnsigned(IdUtil.murmurHashCode(id), 1000000000);
    Assert.assertEquals(remainder, 787616861);
  }

  @Test
  void testConcateIds() {
    String taskHexStr = "123456789ABCDEF123456789ABCDEF00";
    String objectHexStr = taskHexStr + "01020304";
    ObjectId objectId1 = ObjectId.fromHexString(objectHexStr);
    ObjectId objectId2 = ObjectId.fromHexString(objectHexStr);
    TaskId[] taskIds = new TaskId[2];
    taskIds[0] = objectId1.getTaskId();
    taskIds[1] = objectId2.getTaskId();
    ObjectId[] objectIds = new ObjectId[2];
    objectIds[0] = objectId1;
    objectIds[1] = objectId2;
    String taskHexCompareStr = taskHexStr + taskHexStr;
    String objectHexCompareStr = objectHexStr + objectHexStr;
    Assert.assertEquals(DatatypeConverter.printHexBinary(
        IdUtil.concatIds(taskIds).array()), taskHexCompareStr);
    Assert.assertEquals(DatatypeConverter.printHexBinary(
        IdUtil.concatIds(objectIds).array()), objectHexCompareStr);
  }

}
