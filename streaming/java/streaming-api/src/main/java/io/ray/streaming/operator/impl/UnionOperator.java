package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.internal.Functions;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;

public class UnionOperator<T> extends StreamOperator<Function> implements OneInputOperator<T> {

  public UnionOperator() {
    super(Functions.emptyFunction());
  }

  @Override
  public void processElement(Record<T> record) {
    collect(record);
  }
}
