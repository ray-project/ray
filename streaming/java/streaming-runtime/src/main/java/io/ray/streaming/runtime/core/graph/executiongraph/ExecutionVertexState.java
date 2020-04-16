package io.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

/**
 * Vertex state.
 */
public enum ExecutionVertexState implements Serializable {

  /**
   * Vertex(Worker) to be added.
   */
  TO_ADD(1, "TO_ADD"),

  /**
   * Vertex(Worker) to be deleted.
   */
  TO_DEL(2, "TO_DEL"),

  /**
   * Vertex(Worker) is running.
   */
  RUNNING(3, "RUNNING"),

  /**
   * Unknown status,
   */
  UNKNOWN(-1, "UNKNOWN");

  public final int code;
  public final String msg;

  ExecutionVertexState(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }

}
