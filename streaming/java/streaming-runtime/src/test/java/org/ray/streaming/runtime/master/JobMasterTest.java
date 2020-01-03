package org.ray.streaming.runtime.master;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.runtime.util.TestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JobMasterTest {

  private static final Logger LOG = LoggerFactory.getLogger(JobMasterTest.class);

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
  public void constructionTest() {
    Map<String, String> jobConfig = new HashMap<>();

    JobMaster jobMaster = new JobMaster(jobConfig);

    Assert.assertTrue(jobMaster.getRuntimeContext() != null);
    Assert.assertTrue(jobMaster.getConf() != null);
  }
}
