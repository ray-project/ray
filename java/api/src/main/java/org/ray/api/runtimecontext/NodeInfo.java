package org.ray.api.runtimecontext;

import java.util.Map;
import org.ray.api.id.UniqueId;

/**
 * A class that represents the information of a node.
 */
public class NodeInfo {

  public final UniqueId nodeId;

  public final String nodeAddress;

  public final boolean isAlive;

  public final Map<String, Double> resources;

  public NodeInfo(UniqueId nodeId,  String nodeAddress,
                  boolean isAlive, Map<String, Double> resources) {
    this.nodeId = nodeId;
    this.nodeAddress = nodeAddress;
    this.isAlive = isAlive;
    this.resources = resources;
  }

  public String toString() {
    return "NodeInfo{"
        + "nodeId='" + nodeId + '\''
        + ", nodeAddress='" + nodeAddress + "\'"
        + ", isAlive=" + isAlive
        + ", resources=" + resources
        + "}";
  }

}
