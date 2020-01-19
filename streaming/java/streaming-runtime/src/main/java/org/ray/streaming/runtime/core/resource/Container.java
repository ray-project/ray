package org.ray.streaming.runtime.core.resource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.ray.api.id.UniqueId;

/**
 * Resource manager unit abstraction.
 * Container identifies the available resource(cpu,mem) and allocated slots.
 */
public class Container implements Serializable {

  private ContainerID id;
  private UniqueId nodeId;
  private String address;
  private String hostname;

  private Map<String, Double> availableResource = new HashMap<>();
  private List<Slot> slots = new ArrayList<>();

  public Container() {
  }

  public Container(UniqueId nodeId, String address, String hostname) {
    this.id = new ContainerID();
    this.nodeId = nodeId;
    this.address = address;
    this.hostname = hostname;
  }

  public void setId(ContainerID id) {
    this.id = id;
  }

  public ContainerID getId() {
    return id;
  }

  public String getName() {
    return id.toString();
  }

  public String getAddress() {
    return address;
  }

  public UniqueId getNodeId() {
    return nodeId;
  }

  public String getHostname() {
    return hostname;
  }

  public Map<String, Double> getAvailableResource() {
    return availableResource;
  }

  public void setAvailableResource(Map<String, Double> availableResource) {
    this.availableResource = availableResource;
  }

  public List<Slot> getSlots() {
    return slots;
  }

  public void setSlots(List<Slot> slots) {
    this.slots = slots;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("nodeId", nodeId)
        .add("address", address)
        .add("hostname", hostname)
        .add("availableResource", availableResource)
        .add("slots", slots)
        .toString();
  }
}