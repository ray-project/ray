package org.ray.streaming.plan;

/**
 * Different roles for a node.
 */
public enum VertexType {
  MASTER,
  SOURCE,
  PROCESS,
  SINK,
}
