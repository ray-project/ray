package io.ray.runtime;

import io.ray.api.id.UniqueId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UniqueIdTest {

  @Test
  public void testConstructUniqueId() {
    // Test `fromHexString()`
    UniqueId id1 =
        UniqueId.fromHexString("00000000123456789ABCDEF123456789ABCDEF0123456789ABCDEF00");
    Assert.assertEquals("00000000123456789abcdef123456789abcdef0123456789abcdef00", id1.toString());
    Assert.assertFalse(id1.isNil());

    try {
      UniqueId id2 =
          UniqueId.fromHexString("000000123456789ABCDEF123456789ABCDEF0123456789ABCDEF00");
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
    byte[] bytes =
        DatatypeConverter.parseHexBinary(
            "0123456789ABCDEF0123456789ABCDEF012345670123456789ABCDEF");
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, 28);
    UniqueId id4 = UniqueId.fromByteBuffer(byteBuffer);
    Assert.assertTrue(Arrays.equals(bytes, id4.getBytes()));
    Assert.assertEquals("0123456789abcdef0123456789abcdef012345670123456789abcdef", id4.toString());

    // Test `genNil()`
    UniqueId id6 = UniqueId.NIL;
    Assert.assertEquals(
        "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF".toLowerCase(), id6.toString());
    Assert.assertTrue(id6.isNil());
  }
}
