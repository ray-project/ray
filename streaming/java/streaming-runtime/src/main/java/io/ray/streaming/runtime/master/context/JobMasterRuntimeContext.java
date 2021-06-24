package io.ray.streaming.runtime.master.context;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.master.coordinator.command.BaseWorkerCmd;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Runtime context for job master, which will be stored in backend when saving checkpoint.
 *
 * <p>Including: graph, resource, checkpoint info, etc.
 */
public class JobMasterRuntimeContext implements Serializable {

  /*--------------Checkpoint----------------*/
  public volatile List<Long> checkpointIds = new ArrayList<>();
  public volatile long lastCheckpointId = 0;
  public volatile long lastCpTimestamp = 0;
  public volatile BlockingQueue<BaseWorkerCmd> cpCmds = new LinkedBlockingQueue<>();
  /*--------------Failover----------------*/
  public volatile BlockingQueue<BaseWorkerCmd> foCmds = new ArrayBlockingQueue<>(8192);
  public volatile Set<BaseWorkerCmd> unfinishedFoCmds = Sets.newConcurrentHashSet();
  private StreamingConfig conf;
  private JobGraph jobGraph;
  private volatile ExecutionGraph executionGraph;

  public JobMasterRuntimeContext(StreamingConfig conf) {
    this.conf = conf;
  }

  public String getJobName() {
    return conf.masterConfig.commonConfig.jobName();
  }

  public StreamingConfig getConf() {
    return conf;
  }

  public JobGraph getJobGraph() {
    return jobGraph;
  }

  public void setJobGraph(JobGraph jobGraph) {
    this.jobGraph = jobGraph;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public void setExecutionGraph(ExecutionGraph executionGraph) {
    this.executionGraph = executionGraph;
  }

  public Long getLastValidCheckpointId() {
    if (checkpointIds.isEmpty()) {
      // OL is invalid checkpoint id, worker will pass it
      return 0L;
    }
    return checkpointIds.get(checkpointIds.size() - 1);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobGraph", jobGraph)
        .add("executionGraph", executionGraph)
        .add("conf", conf.getMap())
        .toString();
  }
}
