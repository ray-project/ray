package io.ray.streaming.runtime.transfer;

import static org.testng.Assert.assertEquals;

import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.util.EnvUtil;
import org.testng.annotations.Test;

public class ChannelIdTest extends BaseUnitTest {

  static {
    EnvUtil.loadNativeLibraries();
  }

  @Test
  public void testIdStrToBytes() {
    String idStr = ChannelId.genRandomIdStr();
    assertEquals(idStr.length(), ChannelId.ID_LENGTH * 2);
    assertEquals(ChannelId.idStrToBytes(idStr).length, ChannelId.ID_LENGTH);
  }

}