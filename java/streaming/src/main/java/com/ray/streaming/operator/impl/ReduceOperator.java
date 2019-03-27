package com.ray.streaming.operator.impl;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.api.function.impl.ReduceFunction;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.message.KeyRecord;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import com.ray.streaming.operator.StreamOperator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReduceOperator<K, T> extends StreamOperator<ReduceFunction<T>> implements
    OneInputOperator<T> {

  private Map<K, T> reduceState;

  public ReduceOperator(ReduceFunction<T> reduceFunction) {
    super(reduceFunction);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.reduceState = new HashMap<>();
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    KeyRecord<K, T> keyRecord = (KeyRecord<K, T>) record;
    K key = keyRecord.getKey();
    T value = keyRecord.getValue();
    if (reduceState.containsKey(key)) {
      T oldValue = reduceState.get(key);
      T newValue = this.function.reduce(oldValue, value);
      reduceState.put(key, newValue);
      collect(new Record(newValue));
    } else {
      reduceState.put(key, value);
      collect(record);
    }
  }
}
