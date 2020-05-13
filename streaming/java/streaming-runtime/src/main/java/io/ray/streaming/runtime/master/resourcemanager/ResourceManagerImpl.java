package io.ray.streaming.runtime.master.resourcemanager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.config.StreamingMasterConfig;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import io.ray.streaming.runtime.config.types.ResourceAssignStrategyType;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.Resources;
import io.ray.streaming.runtime.master.JobRuntimeContext;
import io.ray.streaming.runtime.master.resourcemanager.strategy.ResourceAssignStrategy;
import io.ray.streaming.runtime.master.resourcemanager.strategy.ResourceAssignStrategyFactory;
import io.ray.streaming.runtime.util.RayUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
  private ResourceAssignStrategy resourceAssignStrategy;

  /**
   * Resource description information.
   */
  private final Resources resources;

  /**
   * Customized actor number for each container
   */
  private int actorNumPerContainer;

  /**
   * Timing resource updating thread
   */
  private final ScheduledExecutorService resourceUpdater = new ScheduledThreadPoolExecutor(1,
      new ThreadFactoryBuilder().setNameFormat("resource-update-thread").build());

  public ResourceManagerImpl(JobRuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
    StreamingMasterConfig masterConfig = runtimeContext.getConf().masterConfig;

    this.resourceConfig = masterConfig.resourceConfig;
    this.resources = new Resources();
    LOG.info("ResourceManagerImpl begin init, conf is {}, resources are {}.",
        resourceConfig, resources);

    // Init custom resource configurations
    this.actorNumPerContainer = resourceConfig.actorNumPerContainer();

    ResourceAssignStrategyType resourceAssignStrategyType =
        ResourceAssignStrategyType.PIPELINE_FIRST_STRATEGY;
    this.resourceAssignStrategy = ResourceAssignStrategyFactory.getStrategy(
      resourceAssignStrategyType);
    LOG.info("Slot assign strategy: {}.", resourceAssignStrategy.getName());

    //Init resource
    initResource();

    checkAndUpdateResourcePeriodically();

    LOG.info("ResourceManagerImpl init success.");
  }

  @Override
  public ResourceAssignmentView assignResource(List<Container> containers,
      ExecutionGraph executionGraph) {
    return resourceAssignStrategy.assignResource(containers, executionGraph);
  }

  @Override
  public String getName() {
    return resourceAssignStrategy.getName();
  }

  @Override
  public ImmutableList<Container> getRegisteredContainers() {
    LOG.info("Current resource detail: {}.", resources.toString());
    return resources.getRegisteredContainers();
  }

  /**
   * Check the status of ray cluster node and update the internal resource information of
   * streaming system.
   */
  private void checkAndUpdateResource() {
    //Get add&del nodes(node -> container)
    Map<UniqueId, NodeInfo> latestNodeInfos = RayUtils.getAliveNodeInfoMap();

    List<UniqueId> addNodes = latestNodeInfos.keySet().stream()
        .filter(this::isAddedNode).collect(Collectors.toList());

    List<UniqueId> deleteNodes = resources.getRegisteredContainerMap().keySet().stream()
        .filter(nodeId -> !latestNodeInfos.containsKey(nodeId)).collect(Collectors.toList());
    LOG.info("Latest node infos: {}, current containers: {}, add nodes: {}, delete nodes: {}.",
        latestNodeInfos, resources.getRegisteredContainers(), addNodes, deleteNodes);

    if (!addNodes.isEmpty() || !deleteNodes.isEmpty()) {
      LOG.info("Latest node infos from GCS: {}", latestNodeInfos);
      LOG.info("Resource details: {}.", resources.toString());
      LOG.info("Get add nodes info: {}, del nodes info: {}.", addNodes, deleteNodes);

      // unregister containers
      unregisterDeletedContainer(deleteNodes);

      // register containers
      registerNewContainers(addNodes.stream().map(latestNodeInfos::get)
          .collect(Collectors.toList()));
    }
  }

  private void registerNewContainers(List<NodeInfo> nodeInfos) {
    LOG.info("Start to register containers. new add node infos are: {}.", nodeInfos);

    if (nodeInfos == null || nodeInfos.isEmpty()) {
      LOG.info("NodeInfos is null or empty, skip registry.");
      return;
    }

    for (NodeInfo nodeInfo : nodeInfos) {
      registerContainer(nodeInfo);
    }
  }

  private void registerContainer(final NodeInfo nodeInfo) {
    LOG.info("Register container {}.", nodeInfo);

    Container container = Container.from(nodeInfo);

    // failover case: container has already allocated actors
    double availableCapacity = actorNumPerContainer - container.getAllocatedActorNum();


    //Create ray resource.
    Ray.setResource(container.getNodeId(), container.getName(), availableCapacity);
    //Mark container is already registered.
    Ray.setResource(container.getNodeId(), CONTAINER_ENGAGED_KEY, 1);

    // update container's available dynamic resources
    container.getAvailableResources()
      .put(container.getName(), availableCapacity);

    // update register container list
    resources.registerContainer(container);
  }

  private void unregisterDeletedContainer(List<UniqueId> deletedIds) {
    LOG.info("Unregister container, deleted node ids are: {}.", deletedIds);
    if (null == deletedIds || deletedIds.isEmpty()) {
      return;
    }
    resources.unRegisterContainer(deletedIds);
  }

  private void initResource() {
    LOG.info("Init resource.");
    checkAndUpdateResource();
  }

  private void checkAndUpdateResourcePeriodically() {
    long intervalSecond = resourceConfig.resourceCheckIntervalSecond();
    this.resourceUpdater.scheduleAtFixedRate(
        Ray.wrapRunnable(this::checkAndUpdateResource), 0, intervalSecond, TimeUnit.SECONDS);
  }

  private boolean isAddedNode(UniqueId uniqueId) {
    return !resources.getRegisteredContainerMap().containsKey(uniqueId);
  }

}
