package org.ray.streaming.runtime.config;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.util.TestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigTest.class);

  @org.testng.annotations.BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTPattern();
  }

  @org.testng.annotations.BeforeMethod
  public void testBegin(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " begin >>>>>>>>>>>>>>>>>>");
  }

  @org.testng.annotations.AfterMethod
  public void testEnd(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " end >>>>>>>>>>>>>>>>>>");
  }

  @Test
  public void testBaseFunc() {
    // conf using
    CommonConfig commonConfig = ConfigFactory.create(CommonConfig.class);
    Assert.assertTrue(commonConfig.fileEncoding().equals("UTF-8"));

    // override conf
    Map<String, String> customConf = new HashMap<>();
    customConf.put(CommonConfig.FILE_ENCODING, "GBK");
    CommonConfig commonConfig2 = ConfigFactory.create(CommonConfig.class, customConf);
    Assert.assertTrue(commonConfig2.fileEncoding().equals("GBK"));
  }

  @Test
  public void testMapTransformation() {
    Map<String, String> conf = new HashMap<>();
    String encodingType = "GBK";
    conf.put(CommonConfig.FILE_ENCODING, encodingType);

    StreamingConfig config = new StreamingConfig(conf);
    Map<String, String> wholeConfigMap = config.getMap();

    Assert.assertTrue(wholeConfigMap.get(CommonConfig.FILE_ENCODING).equals(encodingType));
  }

  @Test
  public void testCustomConfKeeping() {
    Map<String, String> conf = new HashMap<>();
    String customKey = "test_key";
    String customValue = "test_value";
    conf.put(customKey, customValue);
    StreamingConfig config = new StreamingConfig(conf);
    Assert.assertEquals(config.getMap().get(customKey), customValue);
  }
}
