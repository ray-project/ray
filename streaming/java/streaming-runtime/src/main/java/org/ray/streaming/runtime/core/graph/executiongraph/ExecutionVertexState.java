package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

/**
 * Vertex state.
 */
public enum ExecutionVertexState implements Serializable {

  /**
   * execution vertex state enum
   */
  TO_ADD(0, "TO_ADD"),
  TO_DEL(1, "TO_DEL"),
  TO_UPDATE(2, "TO_UPDATE"),
  RUNNING(3, "RUNNING"),
  UNKNOWN(4, "UNKNOWN");

  public final int code;
  public final String msg;

  ExecutionVertexState(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }
}