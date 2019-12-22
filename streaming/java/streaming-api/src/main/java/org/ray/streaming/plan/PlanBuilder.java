package org.ray.streaming.plan;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.operator.StreamOperator;

public class PlanBuilder {

  private Plan plan;

  private AtomicInteger edgeIdGenerator;
  private List<StreamSink> streamSinkList;

  public PlanBuilder(List<StreamSink> streamSinkList) {
    this.plan = new Plan();
    this.streamSinkList = streamSinkList;
    this.edgeIdGenerator = new AtomicInteger(0);
  }

  public Plan buildPlan() {
    for (StreamSink streamSink : streamSinkList) {
      processStream(streamSink);
    }
    return this.plan;
  }

  private void processStream(Stream stream) {
    int vertexId = stream.getId();
    int parallelism = stream.getParallelism();

    StreamOperator streamOperator = stream.getOperator();
    PlanVertex planVertex = null;

    if (stream instanceof StreamSink) {
      planVertex = new PlanVertex(vertexId, parallelism, VertexType.SINK, streamOperator);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      PlanEdge planEdge = new PlanEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.plan.addEdge(planEdge);
      processStream(parentStream);
    } else if (stream instanceof StreamSource) {
      planVertex = new PlanVertex(vertexId, parallelism, VertexType.SOURCE, streamOperator);
    } else if (stream instanceof DataStream) {
      planVertex = new PlanVertex(vertexId, parallelism, VertexType.PROCESS, streamOperator);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      PlanEdge planEdge = new PlanEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.plan.addEdge(planEdge);
      processStream(parentStream);
    }
    this.plan.addVertex(planVertex);
  }

  private int getEdgeId() {
    return this.edgeIdGenerator.incrementAndGet();
  }

}
