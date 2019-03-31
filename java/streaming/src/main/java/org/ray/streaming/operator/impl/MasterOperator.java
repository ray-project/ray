package org.ray.streaming.operator.impl;

import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;


public class MasterOperator extends StreamOperator {

  public MasterOperator() {
    super(null);
  }

  @Override
  public OperatorType getOpType() {
    return OperatorType.MASTER;
  }
}
