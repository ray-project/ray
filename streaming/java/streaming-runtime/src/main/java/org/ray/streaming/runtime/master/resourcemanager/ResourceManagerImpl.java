package org.ray.streaming.runtime.master.resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.runtimecontext.NodeInfo;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.config.types.SlotAssignStrategyType;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManagerImpl implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerImpl.class);

  //Container used tag
  private static final String CONTAINER_ENGAGED_KEY = "CONTAINER_ENGAGED_KEY";

  /**
   * Job runtime context.
   */
  private JobRuntimeContext runtimeContext;

  /**
   * Resource related configuration.
   */
  private ResourceConfig resourceConfig;

  /**
   * Slot assign strategy.
   */
  private SlotAssignStrategy slotAssignStrategy;

  /**
   * Resource description information.
   */
  private final Resources resources;

  private final ScheduledExecutorService scheduledExecutorService;

  public ResourceManagerImpl(JobRuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    StreamingMasterConfig masterConfig = runtimeContext.getConf().masterConfig;

    this.resourceConfig = masterConfig.resourceConfig;
    this.resources = new Resources(resourceConfig);
    LOG.info("ResourceManagerImpl begin init, conf is {}, resources are {}.",
        resourceConfig, resources);

    SlotAssignStrategyType slotAssignStrategyType = SlotAssignStrategyType.PIPELINE_FIRST_STRATEGY;

    this.slotAssignStrategy = SlotAssignStrategyFactory.getStrategy(slotAssignStrategyType);
    this.slotAssignStrategy.setResources(resources);
    LOG.info("Slot assign strategy: {}.", slotAssignStrategy.getName());

    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
    long intervalSecond = resourceConfig.resourceCheckIntervalSecond();
    this.scheduledExecutorService.scheduleAtFixedRate(
        Ray.wrapRunnable(this::checkAndUpdateResources), 0, intervalSecond, TimeUnit.SECONDS);

    LOG.info("ResourceManagerImpl init success.");
  }

  @Override
  public Map<String, Double> allocateResource(final Container container,
      final Map<String, Double> requireResource) {
    LOG.info("Start to allocate resource for actor with container: {}.", container);

    // allocate resource to actor
    Map<String, Double> resources = new HashMap<>();
    Map<String, Double> containResource = container.getAvailableResource();
    for (Map.Entry<String, Double> entry : containResource.entrySet()) {
      if (requireResource.containsKey(entry.getKey())) {
        double availableResource = entry.getValue() - requireResource.get(entry.getKey());
        entry.setValue(availableResource);
        resources.put(entry.getKey(), requireResource.get(entry.getKey()));
      }
    }

    LOG.info("Allocate resource: {} to container {}.", requireResource, container);
    return resources;
  }

  @Override
  public void deallocateResource(final Container container,
      final Map<String, Double> releaseResource) {
    LOG.info("Deallocating resource for container {}.", container);

    Map<String, Double> containResource = container.getAvailableResource();
    for (Map.Entry<String, Double> entry : containResource.entrySet()) {
      if (releaseResource.containsKey(entry.getKey())) {
        double availableResource = entry.getValue() + releaseResource.get(entry.getKey());
        LOG.info("Release source {}:{}", entry.getKey(), releaseResource.get(entry.getKey()));
        entry.setValue(availableResource);
      }
    }

    LOG.info("Deallocated resource for container {} success.", container);
  }

  @Override
  public List<Container> getRegisteredContainers() {
    return new ArrayList<>(resources.getRegisterContainers());
  }

  @Override
  public SlotAssignStrategy getSlotAssignStrategy() {
    return slotAssignStrategy;
  }

  @Override
  public Resources getResources() {
    return this.resources;
  }

  /**
   * Check the status of ray cluster node and update the internal resource information of
   * streaming system.
   */
  private void checkAndUpdateResources() {
    // get all started nodes
    List<NodeInfo> latestNodeInfos = Ray.getRuntimeContext().getAllNodeInfo();

    List<NodeInfo> addNodes = latestNodeInfos.stream().filter(nodeInfo -> {
      for (Container container : resources.getRegisterContainers()) {
        if (container.getNodeId().equals(nodeInfo.nodeId)) {
          return false;
        }
      }
      return true;
    }).collect(Collectors.toList());

    List<Container> deleteContainers = resources.getRegisterContainers().stream()
        .filter(container -> {
          for (NodeInfo nodeInfo : latestNodeInfos) {
            if (nodeInfo.nodeId.equals(container.getNodeId())) {
              return false;
            }
          }
          return true;
        }).collect(Collectors.toList());
    LOG.info("Latest node infos: {}, current containers: {}, add nodes: {}, delete nodes: {}.",
        latestNodeInfos, resources.getRegisterContainers(), addNodes, deleteContainers);

    //Register new nodes.
    if (!addNodes.isEmpty()) {
      for (NodeInfo node : addNodes) {
        registerContainer(node);
      }
    }
    //Clear deleted nodes
    if (!deleteContainers.isEmpty()) {
      for (Container container : deleteContainers) {
        unregisterContainer(container);
      }
    }
  }

  private void registerContainer(final NodeInfo nodeInfo) {
    LOG.info("Register container {}.", nodeInfo);

    Container container =
        new Container(nodeInfo.nodeId, nodeInfo.nodeAddress, nodeInfo.nodeHostname);
    container.setAvailableResource(nodeInfo.resources);

    //Create ray resource.
    Ray.setResource(container.getNodeId(),
        container.getName(),
        resources.getMaxActorNumPerContainer());
    //Mark container is already registered.
    Ray.setResource(container.getNodeId(),
        CONTAINER_ENGAGED_KEY, 1);

    // update register container list
    resources.getRegisterContainers().add(container);
  }

  private void unregisterContainer(final Container container) {
    LOG.info("Unregister container {}.", container);

    // delete resource with capacity to 0
    Ray.setResource(container.getNodeId(), container.getName(), 0);
    Ray.setResource(container.getNodeId(), CONTAINER_ENGAGED_KEY, 0);

    // remove from container map
    resources.getRegisterContainers().remove(container);
  }
}
