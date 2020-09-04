package io.ray.streaming.operator;

import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;

public interface SourceOperator<T> extends Operator {

  void run();

  SourceContext<T> getSourceContext();

  default OperatorType getOpType() {
    return OperatorType.SOURCE;
  }
}