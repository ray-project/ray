package org.ray.streaming.runtime.master.graphmanager;

import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * The central role of graph management in JobMaster.
 */
public interface GraphManager extends Serializable {

  /**
   * Build execution graph from job graph.
   * @param jobGraph logical plan of streaming job.
   * @return physical plan of streaming job.
   */
  ExecutionGraph buildExecutionGraph(JobGraph jobGraph);

  /**
   * Get job graph.
   *
   * @return the job graph.
   */
  JobGraph getJobGraph();

  /**
   * Get execution graph.
   *
   * @return the execution graph.
   */
  ExecutionGraph getExecutionGraph();

}
