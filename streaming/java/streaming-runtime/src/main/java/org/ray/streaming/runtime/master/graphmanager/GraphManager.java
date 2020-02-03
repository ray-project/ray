package org.ray.streaming.runtime.master.graphmanager;

import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * Graph manager is one of the important roles of JobMaster. It mainly focuses on graph management.
 * Such as: 1) build execution graph from job graph
 *          2) do modifications or operations on graph
 *          3) query vertex info from graph
 */
public interface GraphManager {

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
