package io.ray.streaming.jobgraph;

/** Different roles for a node. */
public enum VertexType {
  SOURCE,
  TRANSFORMATION,
  SINK,
}
