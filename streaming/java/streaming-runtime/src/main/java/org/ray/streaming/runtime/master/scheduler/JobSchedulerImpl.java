package org.ray.streaming.runtime.master.scheduler;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job scheduler implementation.
 */
public class JobSchedulerImpl implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);

  private StreamingConfig jobConf;

  private final JobMaster jobMaster;
  private final ResourceManager resourceManager;
  private final GraphManager graphManager;
  private final WorkerLifecycleController workerLifecycleController;
  private final SlotAssignStrategy strategy;

  public JobSchedulerImpl(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.graphManager = jobMaster.getGraphManager();
    this.resourceManager = jobMaster.getResourceManager();
    this.workerLifecycleController = new WorkerLifecycleController();
    this.strategy = resourceManager.getSlotAssignStrategy();
    this.jobConf = jobMaster.getRuntimeContext().getConf();

    LOG.info("Scheduler init success.");
  }

  @Override
  public boolean scheduleJob(ExecutionGraph executionGraph) {
    LOG.info("Start to schedule job: {}.", executionGraph.getJobName());

    // get max parallelism
    int maxParallelism = executionGraph.getMaxParallelism();

    // get containers
    List<Container> containers = resourceManager.getRegisteredContainers();
    Preconditions.checkState(containers != null && !containers.isEmpty(),
        "containers is invalid: %s", containers);

    // allocate slot
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers, maxParallelism);
    resourceManager.getResources().setSlotNumPerContainer(slotNumPerContainer);
    LOG.info("Slot num per container: {}.", slotNumPerContainer);

    strategy.allocateSlot(containers, slotNumPerContainer);
    LOG.info("Container slot map is: {}.", resourceManager.getResources().getAllocatingMap());

    // assign slot
    Map<ContainerID, List<Slot>> allocatingMap = strategy.assignSlot(executionGraph);
    LOG.info("Allocating map is: {}.", allocatingMap);

    // start all new added workers
    createWorkers(executionGraph);

    // init worker context and start to run
    initAndStart(executionGraph);

    return true;
  }

  /**
   * Init JobMaster and JobWorkers then start JobWorkers.
   *
   * @param executionGraph physical plan
   */
  private void initAndStart(ExecutionGraph executionGraph) {
    // generate vertex - context map
    Map<ExecutionVertex, JobWorkerContext> vertexToContextMap = buildWorkersContext(executionGraph);

    // init workers
    initWorkers(vertexToContextMap);

    // init master
    initMaster();

    // start workers
    startWorkers(executionGraph);
  }

  /**
   * Create JobWorker actors according to the physical plan.
   *
   * @param executionGraph physical plan
   * @return actor creation result
   */
  public boolean createWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin creating workers.");
    long startTs = System.currentTimeMillis();

    // create JobWorker actors
    boolean createResult = workerLifecycleController
        .createWorkers(executionGraph.getAllAddedExecutionVertices());

    if (createResult) {
      LOG.info("Finish creating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return true;
    } else {
      LOG.info("Failed creating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return false;
    }
  }

  /**
   * Init JobWorkers according to the vertex and context infos.
   *
   * @param vertexToContextMap vertex - context map
   */
  protected boolean initWorkers(Map<ExecutionVertex, JobWorkerContext> vertexToContextMap) {
    boolean result;
    try {
      result = workerLifecycleController.initWorkers(vertexToContextMap,
          jobConf.masterConfig.schedulerConfig.workerInitiationWaitTimeoutMs());
    } catch (Exception e) {
      LOG.error("Failed to initiate workers.", e);
      return false;
    }
    return result;
  }

  /**
   * Start JobWorkers according to the physical plan.
   */
  public boolean startWorkers(ExecutionGraph executionGraph) {
    boolean result;
    try {
      result = workerLifecycleController.startWorkers(
          executionGraph, jobConf.masterConfig.schedulerConfig.workerStartingWaitTimeoutMs());
    } catch (Exception e) {
      LOG.error("Failed to start workers.", e);
      return false;
    }
    return result;
  }

  /**
   * Build workers context.
   *
   * @param executionGraph execution graph
   * @return vertex to worker context map
   */
  protected Map<ExecutionVertex, JobWorkerContext> buildWorkersContext(
      ExecutionGraph executionGraph) {
    RayActor masterActor = jobMaster.getJobMasterActor();

    // build workers' context
    Map<ExecutionVertex, JobWorkerContext> needRegistryVertexToContextMap = new HashMap<>();
    executionGraph.getAllExecutionVertices().forEach(vertex -> {
      JobWorkerContext ctx = buildJobWorkerContext(vertex, masterActor);
      needRegistryVertexToContextMap.put(vertex, ctx);
    });
    return needRegistryVertexToContextMap;
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex,
      RayActor<JobMaster> masterActor) {

    // create worker context
    JobWorkerContext ctx = new JobWorkerContext(
        executionVertex.getWorkerActorId(),
        masterActor,
        executionVertex
    );

    return ctx;
  }

  /**
   * Destroy JobWorkers according to the vertex infos.
   *
   * @param executionVertices specified vertices
   */
  public boolean destroyWorkers(List<ExecutionVertex> executionVertices) {
    boolean result;
    try {
      result = workerLifecycleController.destroyWorkers(executionVertices);
    } catch (Exception e) {
      LOG.error("Failed to destroy workers.", e);
      return false;
    }
    return result;
  }

  private void initMaster() {
    jobMaster.init(false);
  }

}
