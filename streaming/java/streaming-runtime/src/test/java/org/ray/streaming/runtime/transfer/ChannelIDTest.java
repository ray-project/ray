package org.ray.streaming.runtime.transfer;

import static org.testng.Assert.assertEquals;

import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.util.EnvUtil;
import org.testng.annotations.Test;

public class ChannelIDTest extends BaseUnitTest {

  static {
    EnvUtil.loadNativeLibraries();
  }

  @Test
  public void testIdStrToBytes() {
    String idStr = ChannelID.genRandomIdStr();
    assertEquals(idStr.length(), ChannelID.ID_LENGTH * 2);
    assertEquals(ChannelID.idStrToBytes(idStr).length, ChannelID.ID_LENGTH);
  }

}