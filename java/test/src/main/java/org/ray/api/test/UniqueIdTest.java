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
    TaskId taskId = TaskId.fromHexString("123456789ABCDE123456789ABCDE");

    ObjectId returnId = ObjectId.forReturn(taskId, 1);
    Assert.assertEquals("123456789abcde123456789abcde00c001000000", returnId.toString());
    Assert.assertEquals(returnId.getTaskId(), taskId);

    returnId = ObjectId.forReturn(taskId, 0x01020304);
    Assert.assertEquals("123456789abcde123456789abcde00c004030201", returnId.toString());
  }

  @Test
  public void testComputePutId() {
    // Mock a taskId, the lowest 4 bytes should be 0.
    TaskId taskId = TaskId.fromHexString("123456789ABCDE123456789ABCDE");

    ObjectId putId = ObjectId.forPut(taskId, 1);
    Assert.assertEquals("123456789abcde123456789abcde008001000000".toLowerCase(), putId.toString());

    putId = ObjectId.forPut(taskId, 0x01020304);
    Assert.assertEquals("123456789abcde123456789abcde008004030201".toLowerCase(), putId.toString());
  }

  @Test
  void testMurmurHash() {
    UniqueId id = UniqueId.fromHexString("3131313131313131313132323232323232323232");
    long remainder = Long.remainderUnsigned(IdUtil.murmurHashCode(id), 1000000000);
    Assert.assertEquals(remainder, 787616861);
  }

}
