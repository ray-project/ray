package org.ray.streaming.operator;

import java.io.Serializable;
import java.util.List;
import org.ray.streaming.api.Language;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.api.function.Function;

public interface Operator extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  Function getFunction();

  Language getLanguage();

  OperatorType getOpType();
}
