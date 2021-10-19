package io.ray.streaming.operator;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import java.io.Serializable;
import java.util.List;

public interface Operator extends Serializable {

  String getName();

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  Function getFunction();

  Language getLanguage();

  OperatorType getOpType();

  ChainStrategy getChainStrategy();

  /** See {@link Function#saveCheckpoint()}. */
  Serializable saveCheckpoint();

  /** See {@link Function#loadCheckpoint(Serializable)}. */
  void loadCheckpoint(Serializable checkpointObject);
}
