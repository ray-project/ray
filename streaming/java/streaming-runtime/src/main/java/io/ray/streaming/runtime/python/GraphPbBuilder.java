package io.ray.streaming.runtime.python;

import com.google.protobuf.ByteString;
import io.ray.runtime.actor.NativeRayActor;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonPartition;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.generated.Streaming;
import io.ray.streaming.runtime.serialization.MsgPackSerializer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GraphPbBuilder {

  private MsgPackSerializer serializer = new MsgPackSerializer();

  public RemoteCall.SubExecutionGraph buildSubExecutionGraph(
      ExecutionVertex executionVertex) {
    RemoteCall.SubExecutionGraph.Builder builder =
        RemoteCall.SubExecutionGraph.newBuilder();

    // build vertex
    builder.setCurrentVertex(buildVertex(executionVertex));

    // build upstream vertices
    List<ExecutionVertex> upstreamVertices = executionVertex.getInputVertices();
    List<RemoteCall.SubExecutionGraph.ExecutionVertex> upstreamVertexPbs =
        upstreamVertices.stream()
        .map(this::buildVertex)
        .collect(Collectors.toList());
    builder.addAllUpstreamVertices(upstreamVertexPbs);

    // build downstream vertices
    List<ExecutionVertex> downstreamVertices = executionVertex.getOutputVertices();
    List<RemoteCall.SubExecutionGraph.ExecutionVertex> downstreamVertexPbs =
        downstreamVertices.stream()
            .map(this::buildVertex)
            .collect(Collectors.toList());
    builder.addAllDownstreamVertices(downstreamVertexPbs);

    // build input edges
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    List<RemoteCall.SubExecutionGraph.ExecutionEdge> inputEdgesPbs =
        inputEdges.stream()
            .map(this::buildEdge)
            .collect(Collectors.toList());
    builder.addAllInputEdges(inputEdgesPbs);

    // build output edges
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();
    List<RemoteCall.SubExecutionGraph.ExecutionEdge> outputEdgesPbs =
        outputEdges.stream()
            .map(this::buildEdge)
            .collect(Collectors.toList());
    builder.addAllOutputEdges(outputEdgesPbs);

    return builder.build();
  }

  private RemoteCall.SubExecutionGraph.ExecutionVertex buildVertex(
      ExecutionVertex executionVertex) {
    // build vertex infos
    RemoteCall.SubExecutionGraph.ExecutionVertex.Builder vertexBuilder =
        RemoteCall.SubExecutionGraph.ExecutionVertex.newBuilder();
    vertexBuilder.setVertexId(executionVertex.getId());
    vertexBuilder.setJobVertexId(executionVertex.getJobVertexId());
    vertexBuilder.setJobVertexName(executionVertex.getJobVertexName());
    vertexBuilder.setVertexIndex(executionVertex.getVertexIndex());
    vertexBuilder.setParallelism(executionVertex.getParallelism());
    vertexBuilder.setFunction(
        ByteString.copyFrom(
            serializeFunction(executionVertex.getStreamOperator().getFunction())));
    vertexBuilder.setWorkerActor(
        ByteString.copyFrom(
            ((NativeRayActor) (executionVertex.getWorkerActor())).toBytes()));
    vertexBuilder.setContainerId(executionVertex.getContainerId().toString());
    vertexBuilder.setBuildTime(executionVertex.getBuildTime());
    vertexBuilder.setLanguage(Streaming.Language.valueOf(executionVertex.getLanguage().name()));
    vertexBuilder.putAllConfig(executionVertex.getWorkerConfig());
    vertexBuilder.putAllResource(executionVertex.getResource());

    return vertexBuilder.build();
  }

  private RemoteCall.SubExecutionGraph.ExecutionEdge buildEdge(ExecutionEdge executionEdge) {
    // build edge infos
    RemoteCall.SubExecutionGraph.ExecutionEdge.Builder edgeBuilder =
        RemoteCall.SubExecutionGraph.ExecutionEdge.newBuilder();
    edgeBuilder.setSourceVertexId(executionEdge.getSourceVertexId());
    edgeBuilder.setTargetVertexId(executionEdge.getTargetVertexId());
    edgeBuilder.setPartition(ByteString.copyFrom(serializePartition(executionEdge.getPartition())));

    return edgeBuilder.build();
  }

  private byte[] serializeFunction(Function function) {
    if (function instanceof PythonFunction) {
      PythonFunction pyFunc = (PythonFunction) function;
      // function_bytes, module_name, function_name, function_interface
      return serializer.serialize(Arrays.asList(
          pyFunc.getFunction(), pyFunc.getModuleName(),
          pyFunc.getFunctionName(), pyFunc.getFunctionInterface()
      ));
    } else {
      return new byte[0];
    }
  }

  private byte[] serializePartition(Partition partition) {
    if (partition instanceof PythonPartition) {
      PythonPartition pythonPartition = (PythonPartition) partition;
      // partition_bytes, module_name, function_name
      return serializer.serialize(Arrays.asList(
          pythonPartition.getPartition(), pythonPartition.getModuleName(),
          pythonPartition.getFunctionName()
      ));
    } else {
      return new byte[0];
    }
  }

}
