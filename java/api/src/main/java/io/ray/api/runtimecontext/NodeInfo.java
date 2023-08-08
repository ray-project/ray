package io.ray.api.runtimecontext;

import io.ray.api.id.UniqueId;
import java.util.Map;

/** A class that represents the information of a node. */
public class NodeInfo {

  public final UniqueId nodeId;

  public final String nodeAddress;

  public final String nodeHostname;

  public final int nodeManagerPort;

  public final String objectStoreSocketName;

  public final String rayletSocketName;

  public final boolean isAlive;

  public final Map<String, Double> resources;

  public final Map<String, String> labels;

  public NodeInfo(
      UniqueId nodeId,
      String nodeAddress,
      String nodeHostname,
      int nodeManagerPort,
      String objectStoreSocketName,
      String rayletSocketName,
      boolean isAlive,
      Map<String, Double> resources,
      Map<String, String> labels) {
    this.nodeId = nodeId;
    this.nodeAddress = nodeAddress;
    this.nodeHostname = nodeHostname;
    this.nodeManagerPort = nodeManagerPort;
    this.objectStoreSocketName = objectStoreSocketName;
    this.rayletSocketName = rayletSocketName;
    this.isAlive = isAlive;
    this.resources = resources;
    this.labels = labels;
  }

  public String toString() {
    return "NodeInfo{"
        + "nodeId='"
        + nodeId
        + '\''
        + ", nodeAddress='"
        + nodeAddress
        + "\'"
        + ", nodeHostname'"
        + nodeHostname
        + "\'"
        + ", isAlive="
        + isAlive
        + ", resources="
        + resources
        + ", labels="
        + labels
        + "}";
  }
}
