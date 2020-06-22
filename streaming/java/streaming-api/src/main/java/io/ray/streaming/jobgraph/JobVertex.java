package io.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.Language;
import io.ray.streaming.operator.StreamOperator;
import java.io.Serializable;
import java.util.Map;

/**
 * Job vertex is a cell node where logic is executed.
 */
public class JobVertex implements Serializable {

  private int vertexId;
  private int parallelism;
  private VertexType vertexType;
  private Language language;
  private StreamOperator streamOperator;
  private Map<String, String> config;

  public JobVertex(int vertexId,
                   int parallelism,
                   VertexType vertexType,
                   StreamOperator streamOperator,
                   Map<String, String> config) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.streamOperator = streamOperator;
    this.language = streamOperator.getLanguage();
    this.config = config;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexType", vertexType)
        .add("language", language)
        .add("streamOperator", streamOperator)
        .add("config", config)
        .toString();
  }

}
