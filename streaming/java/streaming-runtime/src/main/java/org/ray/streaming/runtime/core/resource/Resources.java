package org.ray.streaming.runtime.core.resource;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.streaming.runtime.config.master.ResourceConfig;

/**
 * Resource description of ResourceManager.
 */
public class Resources implements Serializable {

  /**
   * Available containers registered to ResourceManager.
   */
  private List<Container> registerContainers = new ArrayList<>();

  /**
   * Mapping of allocated container to slots.
   */
  private Map<ContainerID, List<Slot>> allocatingMap = new HashMap<>(16);

  /**
   * Number of slots per container.
   */
  private int slotNumPerContainer = 0;

  /**
   * Number of actors per container.
   */
  private int actorPerContainer = 0;

  /**
   * Number of actors that the current container has allocated.
   */
  private int currentContainerAllocatedActorNum = 0;

  /**
   * The container index currently being allocated.
   */
  private int currentContainerIndex = 0;

  private int maxActorNumPerContainer;


  public Resources(ResourceConfig resourceConfig) {
    maxActorNumPerContainer = resourceConfig.maxActorNumPerContainer();
  }

  public List<Container> getRegisterContainers() {
    return registerContainers;
  }

  public void setSlotNumPerContainer(int slotNumPerContainer) {
    this.slotNumPerContainer = slotNumPerContainer;
  }

  public int getSlotNumPerContainer() {
    return slotNumPerContainer;
  }

  public void setRegisterContainers(
      List<Container> registerContainers) {
    this.registerContainers = registerContainers;
  }

  public void setCurrentContainerIndex(int currentContainerIndex) {
    this.currentContainerIndex = currentContainerIndex;
  }

  public int getCurrentContainerIndex() {
    return currentContainerIndex;
  }

  public void setCurrentContainerAllocatedActorNum(int currentContainerAllocatedActorNum) {
    this.currentContainerAllocatedActorNum = currentContainerAllocatedActorNum;
  }

  public int getCurrentContainerAllocatedActorNum() {
    return currentContainerAllocatedActorNum;
  }

  public int getActorPerContainer() {
    return actorPerContainer;
  }

  public void setActorPerContainer(int actorPerContainer) {
    this.actorPerContainer = actorPerContainer;
  }

  public Container getRegisterContainerByContainerId(ContainerID containerID) {
    return registerContainers.stream()
        .filter(container -> container.getContainerId().equals(containerID))
        .findFirst().get();
  }

  public int getMaxActorNumPerContainer() {
    return maxActorNumPerContainer;
  }

  public Map<ContainerID, List<Slot>> getAllocatingMap() {
    return allocatingMap;
  }

  public void setAllocatingMap(
      Map<ContainerID, List<Slot>> allocatingMap) {
    this.allocatingMap = allocatingMap;
  }

  public Map<UniqueId, Map<String, Double>> getAllAvailableResource() {
    Map<UniqueId, Map<String, Double>> availableResource = new HashMap<>();
    for (Container container : registerContainers) {
      availableResource.put(container.getNodeId(), container.getAvailableResource());
    }
    return availableResource;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("registerContainers", registerContainers)
        .add("allocatingMap", allocatingMap)
        .add("slotNumPerContainer", slotNumPerContainer)
        .add("actorPerContainer", actorPerContainer)
        .add("currentContainerAllocatedActorNum", currentContainerAllocatedActorNum)
        .add("currentContainerIndex", currentContainerIndex)
        .add("maxActorNumPerContainer", maxActorNumPerContainer)
        .toString();
  }
}
