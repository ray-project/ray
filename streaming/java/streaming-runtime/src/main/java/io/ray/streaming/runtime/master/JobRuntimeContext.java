package io.ray.streaming.runtime.master;

import com.google.common.base.MoreObjects;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import java.io.Serializable;

/**
 * Runtime context for job master.
 *
 * <p>Including: graph, resource, checkpoint info, etc.
 */
public class JobRuntimeContext implements Serializable {

  private StreamingConfig conf;
  private JobGraph jobGraph;
  private volatile ExecutionGraph executionGraph;

  public JobRuntimeContext(StreamingConfig conf) {
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobGraph", jobGraph)
        .add("executionGraph", executionGraph)
        .add("conf", conf.getMap())
        .toString();
  }
}
