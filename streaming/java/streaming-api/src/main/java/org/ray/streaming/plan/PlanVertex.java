package org.ray.streaming.plan;

import java.io.Serializable;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.PythonOperator;

/**
 * PlanVertex is a cell node where logic is executed.
 */
public class PlanVertex implements Serializable {

  private int vertexId;
  private int parallelism;
  private VertexType vertexType;
  private Language language;
  private StreamOperator streamOperator;

  public PlanVertex(int vertexId, int parallelism, VertexType vertexType,
      StreamOperator streamOperator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.streamOperator = streamOperator;
    if (streamOperator instanceof PythonOperator) {
      language = Language.PYTHON;
    } else {
      language = Language.JAVA;
    }
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
    return "PlanVertex{" +
        "vertexId=" + vertexId +
        ", parallelism=" + parallelism +
        ", vertexType=" + vertexType +
        ", language=" + language +
        ", streamOperator=" + streamOperator +
        '}';
  }
}
