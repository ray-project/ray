package io.ray.streaming.operator;

import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;

public interface SourceOperator<T> extends Operator {

  void fetch();

  SourceContext<T> getSourceContext();

  default OperatorType getOpType() {
    return OperatorType.SOURCE;
  }
}
