package io.ray.streaming.operator;

/** Chain strategy for streaming operators. Chained operators are run in the same thread. */
public enum ChainStrategy {
  /**
   * The operator won't be chained with preceding operators, but maybe chained with succeeding
   * operators.
   */
  HEAD,
  /** Operators will be chained together when possible. */
  ALWAYS,
  /** The operator won't be chained with any operator. */
  NEVER
}
