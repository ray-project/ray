package org.ray.streaming.operator.impl;

import org.ray.streaming.api.function.impl.KeyFunction;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.OneInputOperator;
import org.ray.streaming.operator.StreamOperator;

public class KeyByOperator<T, K> extends StreamOperator<KeyFunction<T, K>> implements
    OneInputOperator<T> {

  public KeyByOperator(KeyFunction<T, K> keyFunction) {
    super(keyFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    K key = this.function.keyBy(record.getValue());
    collect(new KeyRecord<>(key, record.getValue()));
  }
}

