package org.ray.streaming.runtime.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;

/**
 * RayUtils is the utility class to access ray runtime api.
 */
public class RayUtils {

  /**
   * Get all node info from GCS
   *
   * @return node info list
   */
  public static List<NodeInfo> getAllNodeInfo() {
    return Ray.getRuntimeContext().getAllNodeInfo();
  }

  /**
   * Get all alive node info map
   *
   * @return node info map, key is unique node id , value is node info
   */
  public static Map<UniqueId, NodeInfo> getAliveNodeInfoMap() {
    return getAllNodeInfo().stream()
      .filter(nodeInfo -> nodeInfo.isAlive)
      .collect(Collectors.toMap(nodeInfo -> nodeInfo.nodeId, nodeInfo -> nodeInfo));
  }
}
