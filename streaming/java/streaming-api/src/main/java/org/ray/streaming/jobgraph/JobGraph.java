package org.ray.streaming.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job graph, the logical plan of streaming job.
 */
public class JobGraph implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobGraph.class);

  private List<JobVertex> jobVertexList;
  private List<JobEdge> jobEdgeList;

  public JobGraph() {
    this.jobVertexList = new ArrayList<>();
    this.jobEdgeList = new ArrayList<>();
  }

  public void addVertex(JobVertex vertex) {
    this.jobVertexList.add(vertex);
  }

  public void addEdge(JobEdge jobEdge) {
    this.jobEdgeList.add(jobEdge);
  }

  public List<JobVertex> getJobVertexList() {
    return jobVertexList;
  }

  public List<JobEdge> getJobEdgeList() {
    return jobEdgeList;
  }

  public String getGraphViz() {
    return "";
  }

  public void printJobGraph() {
    if (!LOGGER.isInfoEnabled()) {
      return;
    }
    LOGGER.info("Printing job graph:");
    for (JobVertex jobVertex : jobVertexList) {
      LOGGER.info(jobVertex.toString());
    }
    for (JobEdge jobEdge : jobEdgeList) {
      LOGGER.info(jobEdge.toString());
    }
  }

}
