package org.ray.streaming.runtime.core.resource;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Slot implements Serializable {
  private int id;
  /**
   * The slot belongs to a container.
   */
  private ContainerID containerID;
  private AtomicInteger actorCount = new AtomicInteger(0);
  /**
   * List of ExecutionVertex ids belong to the slot.
   */
  private List<Integer> executionVertexIds = new ArrayList<>();

  public Slot(int id, ContainerID containerID) {
    this.id = id;
    this.containerID = containerID;
  }

  public int getId() {
    return id;
  }

  public ContainerID getContainerID() {
    return containerID;
  }

  public AtomicInteger getActorCount() {
    return actorCount;
  }

  public List<Integer> getExecutionVertexIds() {
    return executionVertexIds;
  }

  public void setExecutionVertexIds(List<Integer> executionVertexIds) {
    this.executionVertexIds = executionVertexIds;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("containerID", containerID)
        .add("actorCount", actorCount)
        .toString();
  }
}
