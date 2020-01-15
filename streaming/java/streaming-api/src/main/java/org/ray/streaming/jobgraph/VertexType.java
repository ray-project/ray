package org.ray.streaming.jobgraph;

/**
 * Different roles for a node.
 */
public enum VertexType {
  MASTER,
  SOURCE,
  PROCESS,
  SINK,
}
