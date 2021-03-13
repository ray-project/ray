package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DynamicResourceTest extends BaseTest {

  public static String sayHi() {
    return "hi";
  }

  // Dynamic resources not supported yet.
  @Test(
      groups = {"cluster"},
      enabled = false)
  public void testSetResource() {
    // Call a task in advance to warm up the cluster to avoid being too slow to start workers.
    TestUtils.warmUpCluster();

    String resourceName = "A";
    ObjectRef<String> obj =
        Ray.task(DynamicResourceTest::sayHi).setResource(resourceName, 10.0).remote();
    WaitResult<String> result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 0);

    Ray.setResource(resourceName, 10.0);
    boolean resourceReady =
        TestUtils.waitForCondition(
            () -> {
              List<NodeInfo> nodes = Ray.getRuntimeContext().getAllNodeInfo();
              // NOTE: GCS updates node resources asynchronously.
              // If we directly get the value of "A" for comparison without check whether "A" exists
              // or not,
              // it maybe lead to NPE.
              if (nodes.size() != 1 || !nodes.get(0).resources.containsKey(resourceName)) {
                return false;
              }
              return (0 == Double.compare(10.0, nodes.get(0).resources.get(resourceName)));
            },
            2000);

    Assert.assertTrue(resourceReady);

    // Assert ray call result.
    result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 1);
    Assert.assertEquals(obj.get(), "hi");
  }
}
