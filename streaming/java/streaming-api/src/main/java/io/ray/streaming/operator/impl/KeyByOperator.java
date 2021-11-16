package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;

public class KeyByOperator<T, K> extends StreamOperator<KeyFunction<T, K>>
    implements OneInputOperator<T> {

  public KeyByOperator(KeyFunction<T, K> keyFunction) {
    super(keyFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    K key = this.function.keyBy(record.getValue());
    collect(new KeyRecord<>(key, record.getValue()));
  }
}
