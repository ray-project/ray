package org.ray.streaming.operator;

import org.ray.streaming.message.Record;


public interface TwoInputOperator<T, O> extends Operator {

  void processElement(Record<T> record1, Record<O> record2);

  default OperatorType getOpType() {
    return OperatorType.TWO_INPUT;
  }
}
