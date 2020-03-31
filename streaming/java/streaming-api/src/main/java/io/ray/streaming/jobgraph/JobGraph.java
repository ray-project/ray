package io.ray.streaming.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job graph, the logical plan of streaming job.
 */
public class JobGraph implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobGraph.class);

  private final String jobName;
  private final Map<String, String> jobConfig;
  private List<JobVertex> jobVertexList;
  private List<JobEdge> jobEdgeList;
  private String digraph;

  public JobGraph(String jobName, Map<String, String> jobConfig) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
    this.jobVertexList = new ArrayList<>();
    this.jobEdgeList = new ArrayList<>();
  }

  /**
   * Generate direct-graph(made up of a set of vertices and connected by edges)
   * by current job graph for simple log printing.
   *
   * @return Digraph in string type.
   */
  public String generateDigraph() {
    StringBuilder digraph = new StringBuilder();
    digraph.append("digraph ").append(jobName + " ").append(" {");

    for (JobEdge jobEdge : jobEdgeList) {
      String srcNode = null;
      String targetNode = null;
      for (JobVertex jobVertex : jobVertexList) {
        if (jobEdge.getSrcVertexId() == jobVertex.getVertexId()) {
          srcNode = jobVertex.getVertexId() + "-" + jobVertex.getStreamOperator().getName();
        } else if (jobEdge.getTargetVertexId() == jobVertex.getVertexId()) {
          targetNode = jobVertex.getVertexId() + "-" + jobVertex.getStreamOperator().getName();
        }
      }
      digraph.append(System.getProperty("line.separator"));
      digraph.append(srcNode).append(" -> ").append(targetNode);
    }
    digraph.append(System.getProperty("line.separator")).append("}");

    this.digraph = digraph.toString();
    return this.digraph;
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

  public String getDigraph() {
    return digraph;
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public void printJobGraph() {
    if (!LOG.isInfoEnabled()) {
      return;
    }
    LOG.info("Printing job graph:");
    for (JobVertex jobVertex : jobVertexList) {
      LOG.info(jobVertex.toString());
    }
    for (JobEdge jobEdge : jobEdgeList) {
      LOG.info(jobEdge.toString());
    }
  }

}
