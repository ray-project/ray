package com.ray.streaming.operator.impl;

import com.ray.streaming.operator.OperatorType;
import com.ray.streaming.operator.StreamOperator;


public class MasterOperator extends StreamOperator {

  public MasterOperator() {
    super(null);
  }

  @Override
  public OperatorType getOpType() {
    return OperatorType.MASTER;
  }
}
