package io.ray.streaming.runtime.master.graphmanager;

import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * Graph manager is one of the important roles of JobMaster. It mainly focuses on graph management.
 *
 * <p>
 * Such as:
 * <ol>
 * <li>Build execution graph from job graph.</li>
 * <li>Do modifications or operations on graph.</li>
 * <li>Query vertex info from graph.</li>
 * </ol>
 * </p>
 */
public interface GraphManager {

  /**
   * Build execution graph from job graph.
   *
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
