package io.ray.streaming.runtime.master;

import com.google.common.base.Preconditions;
import io.ray.api.RayActor;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.StreamingMasterConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import io.ray.streaming.runtime.master.scheduler.JobSchedulerImpl;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobMaster is the core controller in streaming job as a ray actor. It is responsible for all the
 * controls facing the {@link JobWorker}.
 */
public class JobMaster {

  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

  private JobRuntimeContext runtimeContext;
  private ResourceManager resourceManager;
  private JobSchedulerImpl scheduler;
  private GraphManager graphManager;
  private StreamingMasterConfig conf;

  private RayActor jobMasterActor;

  public JobMaster(Map<String, String> confMap) {
    LOG.info("Job master init with conf: {}.", confMap);

    StreamingConfig streamingConfig = new StreamingConfig(confMap);
    this.conf = streamingConfig.masterConfig;

    // init runtime context
    runtimeContext = new JobRuntimeContext(streamingConfig);

    LOG.info("Job master init success.");
  }

  /**
   * Init JobMaster.
   *
   * @param isRecover if it is recover from state
   * @return init result
   */
  public Boolean init(boolean isRecover) {
    LOG.info("Start to init job master. Is recover: {}.", isRecover);

    if (this.runtimeContext.getExecutionGraph() == null) {
      LOG.error("Init job master failed. Job graphs is null.");
      return false;
    }

    // recover from last checkpoint
    if (isRecover) {
      // TODO
    }

    ExecutionGraph executionGraph = graphManager.getExecutionGraph();
    Preconditions.checkArgument(executionGraph != null, "no execution graph");

    LOG.info("Finish to init job master.");
    return true;
  }

  /**
   * Submit job to run:
   * <ol>
   * <li> Using GraphManager to build physical plan according to the logical plan.</li>
   * <li> Using ResourceManager to manage and allocate the resources.</li>
   * <li> Using JobScheduler to schedule the job to run.</li>
   * </ol>
   *
   * @param jobMasterActor JobMaster actor
   * @param jobGraph logical plan
   * @return submit result
   */
  public boolean submitJob(RayActor<JobMaster> jobMasterActor, JobGraph jobGraph) {
    LOG.info("Start to submit job using logical plan: {}.", jobGraph);

    this.jobMasterActor = jobMasterActor;

    // init manager
    graphManager = new GraphManagerImpl(runtimeContext);
    resourceManager = new ResourceManagerImpl(runtimeContext);

    // build and set graph into runtime context
    ExecutionGraph executionGraph = graphManager.buildExecutionGraph(jobGraph);
    runtimeContext.setExecutionGraph(executionGraph);

    // init scheduler
    try {
      scheduler = new JobSchedulerImpl(this);
      scheduler.scheduleJob(graphManager.getExecutionGraph());
    } catch (Exception e) {
      LOG.error("Failed to submit job.", e);
      return false;
    }
    return true;
  }

  public RayActor getJobMasterActor() {
    return jobMasterActor;
  }

  public JobRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  public GraphManager getGraphManager() {
    return graphManager;
  }

  public StreamingMasterConfig getConf() {
    return conf;
  }

}
