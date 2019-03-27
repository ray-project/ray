package com.ray.streaming.operator.impl;

import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.message.KeyRecord;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.OneInputOperator;
import com.ray.streaming.operator.StreamOperator;

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

