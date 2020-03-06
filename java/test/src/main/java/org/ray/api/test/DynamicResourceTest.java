package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.WaitResult;
import org.ray.api.options.CallOptions;
import org.ray.api.runtimecontext.NodeInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DynamicResourceTest extends BaseTest {

  public static String sayHi() {
    return "hi";
  }

  @Test
  public void testSetResource() {
    TestUtils.skipTestUnderSingleProcess();

    // Call a task in advance to warm up the cluster to avoid being too slow to start workers.
    TestUtils.warmUpCluster();

    CallOptions op1 =
        new CallOptions.Builder().setResources(ImmutableMap.of("A", 10.0)).createCallOptions();
    RayObject<String> obj = Ray.call(DynamicResourceTest::sayHi, op1);
    WaitResult<String> result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 0);

    Ray.setResource("A", 10.0);
    boolean resourceReady = TestUtils.waitForCondition(() -> {
      List<NodeInfo> nodes = Ray.getRuntimeContext().getAllNodeInfo();
      if (nodes.size() != 1) {
        return false;
      }
      return (0 == Double.compare(10.0, nodes.get(0).resources.get("A")));
    }, 2000);

    Assert.assertTrue(resourceReady);

    // Assert ray call result.
    result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 1);
    Assert.assertEquals(Ray.get(obj.getId()), "hi");

  }

}
