package org.ray.streaming.operator.impl;

import org.ray.streaming.api.function.impl.FilterFunction;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.OneInputOperator;
import org.ray.streaming.operator.StreamOperator;

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
