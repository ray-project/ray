package io.ray.test;

import io.ray.api.Ray;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class NodeLabelSchedulingTest {
  public void testEmptyNodeLabels() {
    try {
      Ray.init();
      List<NodeInfo> nodeInfos = Ray.getRuntimeContext().getAllNodeInfo();
      Assert.assertTrue(nodeInfos.size() == 1);
      Map<String, String> labels = new HashMap<>();
      labels.put("ray.io/node-id", nodeInfos.get(0).nodeId.toString());
      Assert.assertEquals(nodeInfos.get(0).labels, labels);
    } finally {
      Ray.shutdown();
    }
  }

  public void testSetNodeLabels() {
    System.setProperty("ray.head-args.0", "--labels={\"gpu_type\":\"A100\",\"azone\":\"azone-1\"}");
    try {
      Ray.init();
      List<NodeInfo> nodeInfos = Ray.getRuntimeContext().getAllNodeInfo();
      Assert.assertTrue(nodeInfos.size() == 1);
      Map<String, String> labels = new HashMap<>();
      labels.put("ray.io/node-id", nodeInfos.get(0).nodeId.toString());
      labels.put("gpu_type", "A100");
      labels.put("azone", "azone-1");
      Assert.assertEquals(nodeInfos.get(0).labels, labels);
    } finally {
      System.clearProperty("ray.head-args.0");
      Ray.shutdown();
    }
  }
}
