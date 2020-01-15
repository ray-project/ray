package org.ray.streaming.runtime.master;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * Runtime context for job master.
 * Including: graph, resource, checkpoint info, etc.
 */
public class JobMasterRuntimeContext implements Serializable {

  private StreamingConfig conf;
  private volatile Graphs graphs;

  public JobMasterRuntimeContext(StreamingConfig conf) {
    this.conf = conf;
  }

  public String getJobName() {
    return conf.masterConfig.commonConfig.jobName();
  }

  public StreamingConfig getConf() {
    return conf;
  }

  public Graphs getGraphs() {
    return graphs;
  }

  public void setGraphs(JobGraph jobGraph, ExecutionGraph executionGraph) {
    this.graphs = new Graphs(jobGraph, executionGraph);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("graphs", graphs.getExecutionGraph())
        .add("conf", conf.getMap())
        .toString();
  }
}