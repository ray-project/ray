package org.ray.streaming.runtime;

import java.lang.reflect.Method;
import org.ray.streaming.runtime.util.TestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(BaseUnitTest.class);

  @BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
  }

  @AfterClass
  public void tearDown() {
    TestHelper.clearUTPattern();
  }

  @BeforeMethod
  public void testBegin(Method method) {
    LOG.info(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " begin >>>>>>>>>>>>>>>>>>>>");
  }

  @AfterMethod
  public void testEnd(Method method) {
    LOG.info(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " end >>>>>>>>>>>>>>>>>>");
  }
}
