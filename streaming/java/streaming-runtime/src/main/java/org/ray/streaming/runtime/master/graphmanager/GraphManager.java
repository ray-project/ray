package org.ray.streaming.runtime.master.graphmanager;

import java.io.Serializable;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/**
 * The central role of graph management in JobMaster.
 */
public interface GraphManager extends Serializable {

  /**
   * Build execution graph from job graph.
   * @param jobGraph logical plan of streaming job
   * @return physical plan of streaming job
   */
  ExecutionGraph buildExecutionGraph(JobGraph jobGraph);

  /**
   * Set up execution vertex.
   * @param jobGraph logical plan of streaming job
   * @return physical plan of streaming job
   */
  ExecutionGraph setupExecutionVertex(JobGraph jobGraph);

  /**
   * Get graphs
   *
   * @return all graphs
   */
  Graphs getGraphs();

  /**
   * Get job graph
   *
   * @return job graph
   */
  JobGraph getJobGraph();

  /**
   * Get current execution graph
   *
   * @return current execution graph
   */
  ExecutionGraph getExecutionGraph();
}
