package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

/**
 * Vertex state.
 */
public enum ExecutionVertexState implements Serializable {

  /**
   * execution vertex state enum
   */
  TO_ADD(1, "TO_ADD"),
  TO_DEL(2, "TO_DEL"),
  RUNNING(3, "RUNNING"),
  UNKNOWN(-1, "UNKNOWN");

  public final int code;
  public final String msg;

  ExecutionVertexState(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }
}