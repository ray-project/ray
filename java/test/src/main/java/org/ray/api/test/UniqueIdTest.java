package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.UniqueID;
import org.ray.core.UniqueIdHelper;

@RunWith(MyRunner.class)
public class UniqueIdTest {

  @Test
  public void testComputeReturnId() {
    // Mock a taskId, and the lowest 4 byte should be 0.
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
    // Mock a taskId, the lowest 4 byte should be 0.
    UniqueID taskId = UniqueID.fromHexString("00000000123456789ABCDEF123456789ABCDEF00");

    UniqueID putId = UniqueIdHelper.computePutId(taskId, 1);
    Assert.assertEquals("FFFFFFFF123456789ABCDEF123456789ABCDEF00", putId.toString());

    putId = UniqueIdHelper.computePutId(taskId, 0x01020304);
    Assert.assertEquals("FCFCFDFE123456789ABCDEF123456789ABCDEF00", putId.toString());
  }

}
