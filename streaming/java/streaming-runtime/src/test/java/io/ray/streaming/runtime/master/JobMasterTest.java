package io.ray.streaming.runtime.master;

import io.ray.streaming.runtime.BaseRayClusterTest;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JobMasterTest extends BaseRayClusterTest {

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
