package org.ray.api.test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.UniqueIdUtil;
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
    UniqueId id6 = UniqueId.genNil();
    Assert.assertEquals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".toLowerCase(), id6.toString());
    Assert.assertTrue(id6.isNil());
  }

  @Test
  public void testComputeReturnId() {
    // Mock a taskId, and the lowest 4 bytes should be 0.
    UniqueId taskId = UniqueId.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");

    UniqueId returnId = UniqueIdUtil.computeReturnId(taskId, 1);
    Assert.assertEquals("01000000123456789abcdef123456789abcdef00", returnId.toString());

    returnId = UniqueIdUtil.computeReturnId(taskId, 0x01020304);
    Assert.assertEquals("04030201123456789abcdef123456789abcdef00", returnId.toString());
  }

  @Test
  public void testComputeTaskId() {
    UniqueId objId = UniqueId.fromHexString("34421980123456789ABCDEF123456789ABCDEF00");
    UniqueId taskId = UniqueIdUtil.computeTaskId(objId);

    Assert.assertEquals("00000000123456789abcdef123456789abcdef00", taskId.toString());
  }

  @Test
  public void testComputePutId() {
    // Mock a taskId, the lowest 4 bytes should be 0.
    UniqueId taskId = UniqueId.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");

    UniqueId putId = UniqueIdUtil.computePutId(taskId, 1);
    Assert.assertEquals("FFFFFFFF123456789ABCDEF123456789ABCDEF00".toLowerCase(), putId.toString());

    putId = UniqueIdUtil.computePutId(taskId, 0x01020304);
    Assert.assertEquals("FCFCFDFE123456789ABCDEF123456789ABCDEF00".toLowerCase(), putId.toString());
  }

  @Test
  public void testUniqueIdsAndByteBufferInterConversion() {
    final int len = 5;
    UniqueId[] ids = new UniqueId[len];
    for (int i = 0; i < len; ++i) {
      ids[i] = UniqueId.randomId();
    }

    ByteBuffer temp = UniqueIdUtil.concatUniqueIds(ids);
    UniqueId[] res = UniqueIdUtil.getUniqueIdsFromByteBuffer(temp);

    for (int i = 0; i < len; ++i) {
      Assert.assertEquals(ids[i], res[i]);
    }
  }

}
