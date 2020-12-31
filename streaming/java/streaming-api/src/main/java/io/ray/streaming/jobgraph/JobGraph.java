package io.ray.streaming.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job graph, the logical plan of streaming job. */
public class JobGraph implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobGraph.class);

  private final String jobName;
  private final Map<String, String> jobConfig;
  private List<JobVertex> jobVertices;
  private List<JobEdge> jobEdges;
  private String digraph;

  public JobGraph(String jobName, Map<String, String> jobConfig) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
    this.jobVertices = new ArrayList<>();
    this.jobEdges = new ArrayList<>();
  }

  public JobGraph(
      String jobName,
      Map<String, String> jobConfig,
      List<JobVertex> jobVertices,
      List<JobEdge> jobEdges) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
    this.jobVertices = jobVertices;
    this.jobEdges = jobEdges;
    generateDigraph();
  }

  /**
   * Generate direct-graph(made up of a set of vertices and connected by edges) by current job graph
   * for simple log printing.
   *
   * <p>Returns Digraph in string type.
   */
  public String generateDigraph() {
    StringBuilder digraph = new StringBuilder();
    digraph.append("digraph ").append(jobName).append(" ").append(" {");

    for (JobEdge jobEdge : jobEdges) {
      String srcNode = null;
      String targetNode = null;
      for (JobVertex jobVertex : jobVertices) {
        if (jobEdge.getSrcVertexId() == jobVertex.getVertexId()) {
          srcNode = jobVertex.getVertexId() + "-" + jobVertex.getStreamOperator().getName();
        } else if (jobEdge.getTargetVertexId() == jobVertex.getVertexId()) {
          targetNode = jobVertex.getVertexId() + "-" + jobVertex.getStreamOperator().getName();
        }
      }
      digraph.append(System.getProperty("line.separator"));
      digraph.append(String.format("  \"%s\" -> \"%s\"", srcNode, targetNode));
    }
    digraph.append(System.getProperty("line.separator")).append("}");

    this.digraph = digraph.toString();
    return this.digraph;
  }

  public void addVertex(JobVertex vertex) {
    this.jobVertices.add(vertex);
  }

  public void addEdge(JobEdge jobEdge) {
    this.jobEdges.add(jobEdge);
  }

  public List<JobVertex> getJobVertices() {
    return jobVertices;
  }

  public List<JobVertex> getSourceVertices() {
    return jobVertices.stream()
        .filter(v -> v.getVertexType() == VertexType.SOURCE)
        .collect(Collectors.toList());
  }

  public List<JobVertex> getSinkVertices() {
    return jobVertices.stream()
        .filter(v -> v.getVertexType() == VertexType.SINK)
        .collect(Collectors.toList());
  }

  public JobVertex getVertex(int vertexId) {
    return jobVertices.stream().filter(v -> v.getVertexId() == vertexId).findFirst().get();
  }

  public List<JobEdge> getJobEdges() {
    return jobEdges;
  }

  public Set<JobEdge> getVertexInputEdges(int vertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getTargetVertexId() == vertexId)
        .collect(Collectors.toSet());
  }

  public Set<JobEdge> getVertexOutputEdges(int vertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getSrcVertexId() == vertexId)
        .collect(Collectors.toSet());
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
    for (JobVertex jobVertex : jobVertices) {
      LOG.info(jobVertex.toString());
    }
    for (JobEdge jobEdge : jobEdges) {
      LOG.info(jobEdge.toString());
    }
  }
}
