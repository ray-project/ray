package org.ray.streaming.runtime.core.master.resourcemanager;

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
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManagerImpl implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerImpl.class);

  //Container used tag
  private static final String CONTAINER_ENGAGED_KEY = "CONTAINER_ENGAGED_KEY";

  private ResourceConfig resourceConfig;
  private SlotAssignStrategy slotAssignStrategy;

  private final JobMaster jobMaster;
  private final Resources resources;

  private final ScheduledExecutorService scheduledExecutorService;

  public ResourceManagerImpl(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    StreamingMasterConfig masterConfig = jobMaster.getRuntimeContext().getConf().masterConfig;

    this.resourceConfig = masterConfig.resourceConfig;
    this.resources = new Resources(resourceConfig);
    LOG.info("ResourceManagerImpl begin init, conf is {}, resources are {}.",
        resourceConfig, resources);

    SlotAssignStrategyType slotAssignStrategyType =
        SlotAssignStrategyType.valueOf(jobMaster.getRuntimeContext().getConf().masterConfig.schedulerConfig.slotAssignStrategy().toUpperCase());
    this.slotAssignStrategy = SlotAssignStrategyFactory.getStrategy(slotAssignStrategyType);
    this.slotAssignStrategy.updateResources(resources);
    LOG.info("Slot assign strategy: {}.", slotAssignStrategy.getName());

    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);

    checkAndUpdateResources();

    if (!TestHelper.isUTPattern()) {
      long intervalSecond = resourceConfig.resourceCheckIntervalSecond();
      this.scheduledExecutorService.scheduleAtFixedRate(Ray.wrapRunnable(this::checkAndUpdateResources),
          0, intervalSecond, TimeUnit.SECONDS);
    }
    LOG.info("ResourceManagerImpl init success.");
  }

  @Override
  public Map<String, Double> allocateResource(final ExecutionVertex executionVertex) {
    //executionVertex -> slot -> container
    Container container = resources.getRegisterContainerByContainerId(
        executionVertex.getSlot().getContainerID());
    LOG.info("Start to allocate resource for actor with container: {}.", container);

    // allocate resource to actor
    Map<String, Double> resources = new HashMap<>();
    resources.put(container.getName(), 1.0);

    LOG.info("Allocate resource to actor [vertexId={}] succeeded with container {}.",
        executionVertex.getVertexId(), container);
    return resources;
  }

  @Override
  public void deallocateResource(final ExecutionVertex executionVertex) {
    LOG.info("Start deallocate resource for actor {}.", executionVertex.getWorkerActorId());

    // TODO: decrease container allocated actor num

    LOG.info("Deallocate resource for actor {} success.", executionVertex.getWorkerActorId());
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
  public void setResources(Resources resources) {
    this.jobMaster.getRuntimeContext().setResources(resources);
  }

  @Override
  public Resources getResources() {
    return this.resources;
  }

  private void checkAndUpdateResources() {
    // get add&del nodes
    List<NodeInfo> latestNodeInfos = RayUtils.getNodeInfoList();

    List<NodeInfo> addNodes = latestNodeInfos.stream().filter(nodeInfo -> {
      for (Container container : resources.getRegisterContainers()) {
        if (container.getNodeId().equals(nodeInfo.nodeId)) {
          return false;
        }
      }
      return true;
    }).collect(Collectors.toList());

    List<Container> deleteContainers = resources.getRegisterContainers().stream().filter(container -> {
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

    Container container = new Container(nodeInfo.nodeId, nodeInfo.nodeAddress, nodeInfo.nodeHostname);
    container.setAvailableResource(nodeInfo.resources);

    // create ray resource
    Ray.setResource(container.getNodeId(), container.getName(), resources.getMaxActorNumPerContainer());
    Ray.setResource(container.getNodeId(), CONTAINER_ENGAGED_KEY, 1);

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
