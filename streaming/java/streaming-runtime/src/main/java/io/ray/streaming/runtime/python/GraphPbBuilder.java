package io.ray.streaming.runtime.python;

import com.google.protobuf.ByteString;
import io.ray.runtime.actor.NativeRayActor;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonPartition;
import io.ray.streaming.runtime.core.graph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.ExecutionNode;
import io.ray.streaming.runtime.core.graph.ExecutionTask;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.generated.Streaming;
import java.util.Arrays;

public class GraphPbBuilder {

  private MsgPackSerializer serializer = new MsgPackSerializer();

  /**
   * For simple scenario, a single ExecutionNode is enough. But some cases may need
   * sub-graph information, so we serialize entire graph.
   */
  public RemoteCall.ExecutionGraph buildExecutionGraphPb(ExecutionGraph graph) {
    RemoteCall.ExecutionGraph.Builder builder = RemoteCall.ExecutionGraph.newBuilder();
    builder.setBuildTime(graph.getBuildTime());
    for (ExecutionNode node : graph.getExecutionNodeList()) {
      RemoteCall.ExecutionGraph.ExecutionNode.Builder nodeBuilder =
          RemoteCall.ExecutionGraph.ExecutionNode.newBuilder();
      nodeBuilder.setNodeId(node.getNodeId());
      nodeBuilder.setParallelism(node.getParallelism());
      nodeBuilder.setNodeType(
          Streaming.NodeType.valueOf(node.getNodeType().name()));
      nodeBuilder.setLanguage(Streaming.Language.valueOf(node.getLanguage().name()));
      byte[] functionBytes = serializeFunction(node.getStreamOperator().getFunction());
      nodeBuilder.setFunction(ByteString.copyFrom(functionBytes));

      // build tasks
      for (ExecutionTask task : node.getExecutionTasks()) {
        RemoteCall.ExecutionGraph.ExecutionTask.Builder taskBuilder =
            RemoteCall.ExecutionGraph.ExecutionTask.newBuilder();
        byte[] serializedActorHandle = ((NativeRayActor) task.getWorker()).toBytes();
        taskBuilder
            .setTaskId(task.getTaskId())
            .setTaskIndex(task.getTaskIndex())
            .setWorkerActor(ByteString.copyFrom(serializedActorHandle));
        nodeBuilder.addExecutionTasks(taskBuilder.build());
      }

      // build edges
      for (ExecutionEdge edge : node.getInputsEdges()) {
        nodeBuilder.addInputEdges(buildEdgePb(edge));
      }
      for (ExecutionEdge edge : node.getOutputEdges()) {
        nodeBuilder.addOutputEdges(buildEdgePb(edge));
      }

      builder.addExecutionNodes(nodeBuilder.build());
    }

    return builder.build();
  }

  private RemoteCall.ExecutionGraph.ExecutionEdge buildEdgePb(ExecutionEdge edge) {
    RemoteCall.ExecutionGraph.ExecutionEdge.Builder edgeBuilder =
        RemoteCall.ExecutionGraph.ExecutionEdge.newBuilder();
    edgeBuilder.setSrcNodeId(edge.getSrcNodeId());
    edgeBuilder.setTargetNodeId(edge.getTargetNodeId());
    edgeBuilder.setPartition(ByteString.copyFrom(serializePartition(edge.getPartition())));
    return edgeBuilder.build();
  }

  private byte[] serializeFunction(Function function) {
    if (function instanceof PythonFunction) {
      PythonFunction pyFunc = (PythonFunction) function;
      // function_bytes, module_name, class_name, function_name, function_interface
      return serializer.serialize(Arrays.asList(
          pyFunc.getFunction(), pyFunc.getModuleName(),
          pyFunc.getClassName(), pyFunc.getFunctionName(),
          pyFunc.getFunctionInterface()
      ));
    } else {
      return new byte[0];
    }
  }

  private byte[] serializePartition(Partition partition) {
    if (partition instanceof PythonPartition) {
      PythonPartition pythonPartition = (PythonPartition) partition;
      // partition_bytes, module_name, class_name, function_name
      return serializer.serialize(Arrays.asList(
          pythonPartition.getPartition(), pythonPartition.getModuleName(),
          pythonPartition.getClassName(), pythonPartition.getFunctionName()
      ));
    } else {
      return new byte[0];
    }
  }

}
