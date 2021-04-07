package io.ray.streaming.runtime.master;

import io.ray.api.Ray;
import io.ray.streaming.runtime.BaseUnitTest;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JobMasterTest extends BaseUnitTest {

  @BeforeMethod
  public void init() {
    // ray init
    Ray.init();
  }

  @AfterMethod
  public void tearDown() {
    Ray.shutdown();
  }

  @Test
  public void testCreation() {
    JobMaster jobMaster = new JobMaster(new HashMap<>());
    Assert.assertNotNull(jobMaster.getRuntimeContext());
    Assert.assertNotNull(jobMaster.getConf());
    Assert.assertNull(jobMaster.getGraphManager());
    Assert.assertNull(jobMaster.getResourceManager());
    Assert.assertNull(jobMaster.getJobMasterActor());
    Assert.assertFalse(jobMaster.init(false));
  }
}
