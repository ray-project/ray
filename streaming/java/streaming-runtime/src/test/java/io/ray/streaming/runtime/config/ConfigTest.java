package io.ray.streaming.runtime.config;

import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.config.global.CommonConfig;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.nustaq.serialization.FSTConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigTest extends BaseUnitTest {

  @Test
  public void testBaseFunc() {
    // conf using
    CommonConfig commonConfig = ConfigFactory.create(CommonConfig.class);
    Assert.assertTrue(commonConfig.jobId().equals("default-job-id"));

    // override conf
    Map<String, String> customConf = new HashMap<>();
    customConf.put(CommonConfig.JOB_ID, "111");
    CommonConfig commonConfig2 = ConfigFactory.create(CommonConfig.class, customConf);
    Assert.assertTrue(commonConfig2.jobId().equals("111"));
  }

  @Test
  public void testMapTransformation() {
    Map<String, String> conf = new HashMap<>();
    String testValue = "222";
    conf.put(CommonConfig.JOB_ID, testValue);

    StreamingConfig config = new StreamingConfig(conf);
    Map<String, String> wholeConfigMap = config.getMap();

    Assert.assertTrue(wholeConfigMap.get(CommonConfig.JOB_ID).equals(testValue));
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

  @Test
  public void testSerialization() {
    Map<String, String> conf = new HashMap<>();
    String customKey = "test_key";
    String customValue = "test_value";
    conf.put(customKey, customValue);
    StreamingConfig config = new StreamingConfig(conf);

    FSTConfiguration fstConf = FSTConfiguration.createDefaultConfiguration();
    byte[] configBytes = fstConf.asByteArray(config);
    StreamingConfig deserializedConfig = (StreamingConfig) fstConf.asObject(configBytes);

    Assert.assertEquals(deserializedConfig.masterConfig.commonConfig.jobId(), "default-job-id");
    Assert.assertEquals(deserializedConfig.getMap().get(customKey), customValue);
  }
}
