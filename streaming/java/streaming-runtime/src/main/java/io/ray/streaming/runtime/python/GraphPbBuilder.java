package io.ray.streaming.runtime.python;

import com.google.protobuf.ByteString;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.operator.Operator;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonOperator.ChainedPythonOperator;
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

  public RemoteCall.ExecutionVertexContext buildExecutionVertexContext(
      ExecutionVertex executionVertex) {
    RemoteCall.ExecutionVertexContext.Builder builder =
        RemoteCall.ExecutionVertexContext.newBuilder();

    // build vertex
    builder.setCurrentExecutionVertex(buildVertex(executionVertex));

    // build upstream vertices
    List<ExecutionVertex> upstreamVertices = executionVertex.getInputVertices();
    List<RemoteCall.ExecutionVertexContext.ExecutionVertex> upstreamVertexPbs =
        upstreamVertices.stream()
        .map(this::buildVertex)
        .collect(Collectors.toList());
    builder.addAllUpstreamExecutionVertices(upstreamVertexPbs);

    // build downstream vertices
    List<ExecutionVertex> downstreamVertices = executionVertex.getOutputVertices();
    List<RemoteCall.ExecutionVertexContext.ExecutionVertex> downstreamVertexPbs =
        downstreamVertices.stream()
            .map(this::buildVertex)
            .collect(Collectors.toList());
    builder.addAllDownstreamExecutionVertices(downstreamVertexPbs);

    // build input edges
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    List<RemoteCall.ExecutionVertexContext.ExecutionEdge> inputEdgesPbs =
        inputEdges.stream()
            .map(this::buildEdge)
            .collect(Collectors.toList());
    builder.addAllInputExecutionEdges(inputEdgesPbs);

    // build output edges
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();
    List<RemoteCall.ExecutionVertexContext.ExecutionEdge> outputEdgesPbs =
        outputEdges.stream()
            .map(this::buildEdge)
            .collect(Collectors.toList());
    builder.addAllOutputExecutionEdges(outputEdgesPbs);

    return builder.build();
  }

  private RemoteCall.ExecutionVertexContext.ExecutionVertex buildVertex(
      ExecutionVertex executionVertex) {
    // build vertex infos
    RemoteCall.ExecutionVertexContext.ExecutionVertex.Builder executionVertexBuilder =
        RemoteCall.ExecutionVertexContext.ExecutionVertex.newBuilder();
    executionVertexBuilder.setExecutionVertexId(executionVertex.getExecutionVertexId());
    executionVertexBuilder.setExecutionJobVertexId(executionVertex.getExecutionJobVertexId());
    executionVertexBuilder.setExecutionJobVertexName(executionVertex.getExecutionJobVertexName());
    executionVertexBuilder.setExecutionVertexIndex(executionVertex.getExecutionVertexIndex());
    executionVertexBuilder.setParallelism(executionVertex.getParallelism());
    executionVertexBuilder.setOperator(
        ByteString.copyFrom(
            serializeOperator(executionVertex.getStreamOperator())));
    executionVertexBuilder.setChained(isPythonChainedOperator(executionVertex.getStreamOperator()));
    executionVertexBuilder.setWorkerActor(
        ByteString.copyFrom(
            ((NativeActorHandle) (executionVertex.getWorkerActor())).toBytes()));
    executionVertexBuilder.setContainerId(executionVertex.getContainerId().toString());
    executionVertexBuilder.setBuildTime(executionVertex.getBuildTime());
    executionVertexBuilder.setLanguage(
        Streaming.Language.valueOf(executionVertex.getLanguage().name()));
    executionVertexBuilder.putAllConfig(executionVertex.getWorkerConfig());
    executionVertexBuilder.putAllResource(executionVertex.getResource());

    return executionVertexBuilder.build();
  }

  private RemoteCall.ExecutionVertexContext.ExecutionEdge buildEdge(ExecutionEdge executionEdge) {
    // build edge infos
    RemoteCall.ExecutionVertexContext.ExecutionEdge.Builder executionEdgeBuilder =
        RemoteCall.ExecutionVertexContext.ExecutionEdge.newBuilder();
    executionEdgeBuilder.setSourceExecutionVertexId(executionEdge.getSourceVertexId());
    executionEdgeBuilder.setTargetExecutionVertexId(executionEdge.getTargetVertexId());
    executionEdgeBuilder.setPartition(
        ByteString.copyFrom(serializePartition(executionEdge.getPartition())));

    return executionEdgeBuilder.build();
  }

  private byte[] serializeOperator(Operator operator) {
    if (operator instanceof PythonOperator) {
      if (isPythonChainedOperator(operator)) {
        return serializePythonChainedOperator((ChainedPythonOperator) operator);
      } else {
        PythonOperator pythonOperator = (PythonOperator) operator;
        return serializer.serialize(Arrays.asList(
            serializeFunction(pythonOperator.getFunction()),
            pythonOperator.getModuleName(),
            pythonOperator.getClassName()
        ));
      }
    } else {
      return new byte[0];
    }
  }

  private boolean isPythonChainedOperator(Operator operator) {
    return operator instanceof ChainedPythonOperator;
  }

  private byte[] serializePythonChainedOperator(ChainedPythonOperator operator) {
    List<byte[]> serializedOperators = operator.getOperators().stream()
        .map(this::serializeOperator).collect(Collectors.toList());
    return serializer.serialize(Arrays.asList(
        serializedOperators,
        operator.getConfigs()
    ));
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
