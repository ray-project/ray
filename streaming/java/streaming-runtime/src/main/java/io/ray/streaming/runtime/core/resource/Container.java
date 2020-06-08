package io.ray.streaming.runtime.core.resource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container is physical resource abstraction. It identifies the available resources(cpu,mem,etc.)
 * and allocated actors.
 */
public class Container implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Container.class);

  /**
   * container id
   */
  private ContainerID id;

  /**
   * Container address
   */
  private String address;

  /**
   * Container hostname
   */
  private String hostname;

  /**
   * Container unique id fetched from raylet
   */
  private UniqueId nodeId;

  /**
   * Container available resources
   */
  private Map<String, Double> availableResources = new HashMap<>();

  /**
   * List of {@link ExecutionVertex} ids
   * belong to the container.
   */
  private List<Integer> executionVertexIds = new ArrayList<>();

  /**
   * Capacity is max actor number could be allocated in the container
   */
  private int capacity = 0;

  public Container() {
  }

  public Container(
      String address,
      UniqueId nodeId, String hostname,
      Map<String, Double> availableResources) {

    this.id = new ContainerID();
    this.address = address;
    this.hostname = hostname;
    this.nodeId = nodeId;
    this.availableResources = availableResources;
  }

  public static Container from(NodeInfo nodeInfo) {
    return new Container(
        nodeInfo.nodeAddress,
        nodeInfo.nodeId,
        nodeInfo.nodeHostname,
        nodeInfo.resources
    );
  }

  public ContainerID getId() {
    return id;
  }

  public void setId(ContainerID id) {
    this.id = id;
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

  public Map<String, Double> getAvailableResources() {
    return availableResources;
  }

  public int getCapacity() {
    return capacity;
  }


  public void updateCapacity(int capacity) {
    LOG.info("Update container capacity, old value: {}, new value: {}.", this.capacity, capacity);
    this.capacity = capacity;
  }

  public int getRemainingCapacity() {
    return capacity - getAllocatedActorNum();
  }

  public int getAllocatedActorNum() {
    return executionVertexIds.size();
  }

  public boolean isFull() {
    return getAllocatedActorNum() >= capacity;
  }

  public boolean isEmpty() {
    return getAllocatedActorNum() == 0;
  }

  public void allocateActor(ExecutionVertex vertex) {
    LOG.info("Allocating vertex [{}] in container [{}].", vertex, this);

    executionVertexIds.add(vertex.getExecutionVertexId());
    vertex.setContainerIfNotExist(this.getId());
    // Binding dynamic resource
    vertex.getResource().put(getName(), 1.0);
    decreaseResource(vertex.getResource());
  }

  public void releaseActor(ExecutionVertex vertex) {
    LOG.info("Release actor, vertex: {}, container: {}.", vertex, vertex.getContainerId());
    if (executionVertexIds.contains(vertex.getExecutionVertexId())) {
      executionVertexIds.removeIf(id -> id == vertex.getExecutionVertexId());
      reclaimResource(vertex.getResource());
    } else {
      throw new RuntimeException(String.format("Current container [%s] not found vertex [%s].",
          this, vertex.getExecutionJobVertexName()));
    }
  }

  public List<Integer> getExecutionVertexIds() {
    return executionVertexIds;
  }

  private void decreaseResource(Map<String, Double> allocatedResource) {
    allocatedResource.forEach((k, v) -> {
      Preconditions.checkArgument(this.availableResources.get(k) >= v,
          String.format("Available resource %s not >= decreased resource %s",
              this.availableResources.get(k), v));
      Double newValue = this.availableResources.get(k) - v;
      LOG.info("Decrease container {} resource [{}], from {} to {}.",
          this.address, k, this.availableResources.get(k), newValue);
      this.availableResources.put(k, newValue);
    });
  }

  private void reclaimResource(Map<String, Double> allocatedResource) {
    allocatedResource.forEach((k, v) -> {
      Double newValue = this.availableResources.get(k) + v;
      LOG.info("Reclaim container {} resource [{}], from {} to {}.",
          this.address, k, this.availableResources.get(k), newValue);
      this.availableResources.put(k, newValue);
    });
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("address", address)
        .add("hostname", hostname)
        .add("nodeId", nodeId)
        .add("availableResources", availableResources)
        .add("executionVertexIds", executionVertexIds)
        .add("capacity", capacity)
        .toString();
  }
}