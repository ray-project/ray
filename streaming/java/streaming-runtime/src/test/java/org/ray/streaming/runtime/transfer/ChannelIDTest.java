package org.ray.streaming.runtime.transfer;

import static org.testng.Assert.assertEquals;


import org.ray.streaming.runtime.TestHelper;
import org.testng.annotations.Test;

public class ChannelIDTest {

  static {
    TestHelper.loadNativeLibraries();
  }

  @Test
  public void testIdStrToBytes() {
    String idStr = ChannelID.genRandomIdStr();
    assertEquals(idStr.length(), ChannelID.ID_LENGTH * 2);
    assertEquals(ChannelID.idStrToBytes(idStr).length, ChannelID.ID_LENGTH);
  }

}