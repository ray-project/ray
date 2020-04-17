package io.ray.streaming.runtime.util;

import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.core.resource.ResourceType;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.powermock.api.mockito.PowerMockito;

/**
 * Mockitools is a tool based on powermock and mokito to mock external service api
 */
public class Mockitools {

  /**
   * Mock GCS get node info api
   */
  public static void mockGscApi() {
    PowerMockito.mockStatic(RayUtils.class);
    PowerMockito.when(RayUtils.getAliveNodeInfoMap())
      .thenReturn(mockGetNodeInfoMap(mockGetAllNodeInfo()));
  }

  /**
   * Mock get all node info from GCS
   * @return
   */
  public static List<NodeInfo> mockGetAllNodeInfo() {
    List<NodeInfo> nodeInfos = new LinkedList<>();

    for (int i = 1; i <= 5; i++) {
      Map<String, Double> resources = new HashMap<>();
      resources.put("MEM", 16.0);
      switch (i) {
        case 1:
          resources.put(ResourceType.CPU.getValue(), 3.0);
          break;
        case 2:
        case 3:
        case 4:
          resources.put(ResourceType.CPU.getValue(), 4.0);
          break;
        case 5:
          resources.put(ResourceType.CPU.getValue(), 2.0);
          break;
      }

      nodeInfos.add(mockNodeInfo(i, resources));
    }
    return nodeInfos;
  }

  /**
   * Mock get node info map
   * @param nodeInfos all node infos fetched from GCS
   * @return node info map, key is node unique id, value is node info
   */
  public static Map<UniqueId, NodeInfo> mockGetNodeInfoMap(List<NodeInfo> nodeInfos) {
    return nodeInfos.stream().filter(nodeInfo -> nodeInfo.isAlive).collect(
      Collectors.toMap(nodeInfo -> nodeInfo.nodeId, nodeInfo -> nodeInfo));
  }

  private static NodeInfo mockNodeInfo(int i, Map<String, Double> resources) {
    return new NodeInfo(
      createNodeId(i),
      "localhost" + i,
      "localhost" + i,
      true,
      resources);
  }

  private static UniqueId createNodeId(int id) {
    byte[] nodeIdBytes = new byte[UniqueId.LENGTH];
    for (int byteIndex = 0; byteIndex < UniqueId.LENGTH; ++byteIndex) {
      nodeIdBytes[byteIndex] = String.valueOf(id).getBytes()[0];
    }
    return new UniqueId(nodeIdBytes);
  }
}
