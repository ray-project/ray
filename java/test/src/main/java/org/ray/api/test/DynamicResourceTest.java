package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.WaitResult;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.CallOptions;
import org.ray.api.runtimecontext.NodeInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DynamicResourceTest extends BaseTest {

  @RayRemote
  public static String sayHi() {
    return "hi";
  }

  @Test
  public void testSetResource() {
    TestUtils.skipTestUnderSingleProcess();
    CallOptions op1 = new CallOptions(ImmutableMap.of("A", 10.0));
    RayObject<String> obj = Ray.call(DynamicResourceTest::sayHi, op1);
    WaitResult<String> result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 0);

    Ray.setResource("A", 10.0);

    // Assert node info.
    List<NodeInfo> nodes = Ray.getRuntimeContext().getAllNodeInfo();
    Assert.assertEquals(nodes.size(), 1);
    Assert.assertEquals(nodes.get(0).resources.get("A"), 10.0);

    // Assert ray call result.
    result = Ray.wait(ImmutableList.of(obj), 1, 1000);
    Assert.assertEquals(result.getReady().size(), 1);
    Assert.assertEquals(Ray.get(obj.getId()), "hi");
  }

}
