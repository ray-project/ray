package org.ray.runtime.mockgcsserver;

import java.util.Map;
import org.ray.api.id.UniqueId;

public class NodeResource {

  public final UniqueId nodeId;

  private final String nodeLabel;

  public Map<String, Double> remainingResources;

  public NodeResource(UniqueId nodeId, Map<String, Double> remainingResources) {
    this.nodeId = nodeId;
    this.nodeLabel = nodeId.toString();
    this.remainingResources = remainingResources;
  }

  public String getNodeLabel() {
    return nodeLabel;
  }
}
