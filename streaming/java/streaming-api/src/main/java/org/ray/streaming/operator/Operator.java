package io.ray.streaming.operator;

import java.io.Serializable;
import java.util.List;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;

public interface Operator extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  Function getFunction();

  Language getLanguage();

  OperatorType getOpType();
}
