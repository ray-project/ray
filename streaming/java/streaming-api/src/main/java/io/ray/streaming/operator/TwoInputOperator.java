package io.ray.streaming.operator;

import io.ray.streaming.message.Record;

public interface TwoInputOperator<L, R> extends Operator {

  void processElement(Record<L> record1, Record<R> record2);

  default OperatorType getOpType() {
    return OperatorType.TWO_INPUT;
  }
}
