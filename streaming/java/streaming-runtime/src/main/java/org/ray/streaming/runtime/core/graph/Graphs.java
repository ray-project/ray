package org.ray.streaming.runtime.core.graph;

import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * Graph content which is kept by job master runtime context.
 */
public class Graphs implements Serializable {

  private JobGraph jobGraph;
  private ExecutionGraph executionGraph;

  public Graphs(JobGraph jobGraph, ExecutionGraph executionGraph) {
    this.jobGraph = jobGraph;
    this.executionGraph = executionGraph;
  }

  public JobGraph getJobGraph() {
    return jobGraph;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }
}
