package io.ray.streaming.runtime.transfer;

import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.util.EnvUtil;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

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