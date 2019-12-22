package org.ray.streaming.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The logical execution plan.
 */
public class Plan implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Plan.class);

  private List<PlanVertex> planVertexList;
  private List<PlanEdge> planEdgeList;

  public Plan() {
    this.planVertexList = new ArrayList<>();
    this.planEdgeList = new ArrayList<>();
  }

  public void addVertex(PlanVertex vertex) {
    this.planVertexList.add(vertex);
  }

  public void addEdge(PlanEdge planEdge) {
    this.planEdgeList.add(planEdge);
  }

  public List<PlanVertex> getPlanVertexList() {
    return planVertexList;
  }

  public List<PlanEdge> getPlanEdgeList() {
    return planEdgeList;
  }

  public String getGraphVizPlan() {
    return "";
  }

  public void printPlan() {
    if (!LOGGER.isInfoEnabled()) {
      return;
    }
    LOGGER.info("Printing logic plan:");
    for (PlanVertex planVertex : planVertexList) {
      LOGGER.info(planVertex.toString());
    }
    for (PlanEdge planEdge : planEdgeList) {
      LOGGER.info(planEdge.toString());
    }
  }

}
