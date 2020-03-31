package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;

public class FilterOperator<T> extends StreamOperator<FilterFunction<T>> implements
    OneInputOperator<T> {

  public FilterOperator(FilterFunction<T> filterFunction) {
    super(filterFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    if (this.function.filter(record.getValue())) {
      this.collect(record);
    }
  }
}
