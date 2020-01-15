package org.ray.streaming.operator;

import org.ray.streaming.message.Record;


public interface OneInputOperator<T> extends Operator {

  void processElement(Record<T> record) throws Exception;

  default OperatorType getOpType() {
    return OperatorType.ONE_INPUT;
  }
}
