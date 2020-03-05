package org.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.streaming.api.Language;
import org.ray.streaming.operator.StreamOperator;

/**
 * Job vertex is a cell node where logic is executed.
 */
public class JobVertex implements Serializable {

  private int vertexId;
  private int parallelism;
  private VertexType vertexType;
  private Language language;
  private StreamOperator streamOperator;

  public JobVertex(int vertexId, int parallelism, VertexType vertexType,
      StreamOperator streamOperator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.streamOperator = streamOperator;
    this.language = streamOperator.getLanguage();
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexType", vertexType)
        .add("language", language)
        .add("streamOperator", streamOperator)
        .toString();
  }

}
