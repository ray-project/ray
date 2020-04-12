package io.ray.api.runtimecontext;

import io.ray.api.id.UniqueId;
import java.util.Map;

/**
 * A class that represents the information of a node.
 */
public class NodeInfo {

  public final UniqueId nodeId;

  public final String nodeAddress;

  public final String nodeHostname;

  public final boolean isAlive;

  public final Map<String, Double> resources;

  public NodeInfo(UniqueId nodeId,  String nodeAddress, String nodeHostname,
                  boolean isAlive, Map<String, Double> resources) {
    this.nodeId = nodeId;
    this.nodeAddress = nodeAddress;
    this.nodeHostname = nodeHostname;
    this.isAlive = isAlive;
    this.resources = resources;
  }

  public String toString() {
    return "NodeInfo{"
        + "nodeId='" + nodeId + '\''
        + ", nodeAddress='" + nodeAddress + "\'"
        + ", nodeHostname'" + nodeHostname + "\'"
        + ", isAlive=" + isAlive
        + ", resources=" + resources
        + "}";
  }

}
