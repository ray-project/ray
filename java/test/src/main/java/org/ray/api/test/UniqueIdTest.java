package org.ray.api.test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.UniqueID;
import org.ray.core.UniqueIdHelper;

@RunWith(MyRunner.class)
public class UniqueIdTest {

  @Test
  public void testConstructUniqueId() {
    // Test `fromHexString()`
    UniqueID id1 = UniqueID.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");
    Assert.assertEquals("00000000123456789ABCDEF123456789ABCDEF00", id1.toString());
    Assert.assertFalse(id1.isNil());

    try {
      UniqueID id2 = UniqueID.fromHexString("000000123456789ABCDEF123456789ABCDEF00");
      // This shouldn't be happened.
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    try {
      UniqueID id3 = UniqueID.fromHexString("GGGGGGGGGGGGG");
      // This shouldn't be happened.
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    // Test `fromByteBuffer()`
    byte[] bytes = DatatypeConverter.parseHexBinary("0123456789ABCDEF0123456789ABCDEF01234567");
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, 20);
    UniqueID id4 = UniqueID.fromByteBuffer(byteBuffer);
    Assert.assertTrue(Arrays.equals(bytes, id4.getBytes()));
    Assert.assertEquals("0123456789ABCDEF0123456789ABCDEF01234567", id4.toString());


    // Test `genNil()`
    UniqueID id6 = UniqueID.genNil();
    Assert.assertEquals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", id6.toString());
    Assert.assertTrue(id6.isNil());
  }

  @Test
  public void testComputeReturnId() {
    // Mock a taskId, and the lowest 4 bytes should be 0.
    UniqueID taskId = UniqueID.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");

    UniqueID returnId = UniqueIdHelper.computeReturnId(taskId, 1);
    Assert.assertEquals("01000000123456789ABCDEF123456789ABCDEF00", returnId.toString());

    returnId = UniqueIdHelper.computeReturnId(taskId, 0x01020304);
    Assert.assertEquals("04030201123456789ABCDEF123456789ABCDEF00", returnId.toString());
  }

  @Test
  public void testComputeTaskId() {
    UniqueID objId = UniqueID.fromHexString("34421980123456789ABCDEF123456789ABCDEF00");
    UniqueID taskId = UniqueIdHelper.computeTaskId(objId);

    Assert.assertEquals("00000000123456789ABCDEF123456789ABCDEF00", taskId.toString());
  }

  @Test
  public void testComputePutId() {
    // Mock a taskId, the lowest 4 bytes should be 0.
    UniqueID taskId = UniqueID.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");

    UniqueID putId = UniqueIdHelper.computePutId(taskId, 1);
    Assert.assertEquals("FFFFFFFF123456789ABCDEF123456789ABCDEF00", putId.toString());

    putId = UniqueIdHelper.computePutId(taskId, 0x01020304);
    Assert.assertEquals("FCFCFDFE123456789ABCDEF123456789ABCDEF00", putId.toString());
  }

}
