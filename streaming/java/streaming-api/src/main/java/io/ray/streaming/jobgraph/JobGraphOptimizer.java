package io.ray.streaming.jobgraph;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.ForwardPartition;
import io.ray.streaming.api.partition.impl.RoundRobinPartition;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.chain.ChainedOperator;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonOperator.ChainedPythonOperator;
import io.ray.streaming.python.PythonPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Optimize job graph by chaining some operators so that some operators can be run in the same
 * thread.
 */
public class JobGraphOptimizer {

  private final JobGraph jobGraph;
  private Set<JobVertex> visited = new HashSet<>();
  // vertex id -> vertex
  private Map<Integer, JobVertex> vertexMap;
  private Map<JobVertex, Set<JobEdge>> outputEdgesMap;
  // tail vertex id -> mergedVertex
  private Map<Integer, Pair<JobVertex, List<JobVertex>>> mergedVertexMap;

  public JobGraphOptimizer(JobGraph jobGraph) {
    this.jobGraph = jobGraph;
    vertexMap =
        jobGraph.getJobVertices().stream()
            .collect(Collectors.toMap(JobVertex::getVertexId, Function.identity()));
    outputEdgesMap =
        vertexMap.keySet().stream()
            .collect(
                Collectors.toMap(
                    id -> vertexMap.get(id),
                    id -> new HashSet<>(jobGraph.getVertexOutputEdges(id))));
    mergedVertexMap = new HashMap<>();
  }

  public JobGraph optimize() {
    // Deep-first traverse nodes from source to sink to merge vertices that can be chained
    // together.
    jobGraph
        .getSourceVertices()
        .forEach(
            vertex -> {
              List<JobVertex> verticesToMerge = new ArrayList<>();
              verticesToMerge.add(vertex);
              mergeVerticesRecursively(vertex, verticesToMerge);
            });

    List<JobVertex> vertices =
        mergedVertexMap.values().stream().map(Pair::getLeft).collect(Collectors.toList());

    return new JobGraph(jobGraph.getJobName(), jobGraph.getJobConfig(), vertices, createEdges());
  }

  private void mergeVerticesRecursively(JobVertex vertex, List<JobVertex> verticesToMerge) {
    if (!visited.contains(vertex)) {
      visited.add(vertex);
      Set<JobEdge> outputEdges = outputEdgesMap.get(vertex);
      if (outputEdges.isEmpty()) {
        mergeAndAddVertex(verticesToMerge);
      } else {
        outputEdges.forEach(
            edge -> {
              JobVertex succeedingVertex = vertexMap.get(edge.getTargetVertexId());
              if (canBeChained(vertex, succeedingVertex, edge)) {
                verticesToMerge.add(succeedingVertex);
                mergeVerticesRecursively(succeedingVertex, verticesToMerge);
              } else {
                mergeAndAddVertex(verticesToMerge);
                List<JobVertex> newMergedVertices = new ArrayList<>();
                newMergedVertices.add(succeedingVertex);
                mergeVerticesRecursively(succeedingVertex, newMergedVertices);
              }
            });
      }
    }
  }

  private void mergeAndAddVertex(List<JobVertex> verticesToMerge) {
    JobVertex mergedVertex;
    JobVertex headVertex = verticesToMerge.get(0);
    Language language = headVertex.getLanguage();
    if (verticesToMerge.size() == 1) {
      // no chain
      mergedVertex = headVertex;
    } else {
      List<StreamOperator> operators =
          verticesToMerge.stream()
              .map(v -> vertexMap.get(v.getVertexId()).getStreamOperator())
              .collect(Collectors.toList());
      List<Map<String, String>> configs =
          verticesToMerge.stream()
              .map(v -> vertexMap.get(v.getVertexId()).getConfig())
              .collect(Collectors.toList());
      StreamOperator operator;
      if (language == Language.JAVA) {
        operator = ChainedOperator.newChainedOperator(operators, configs);
      } else {
        List<PythonOperator> pythonOperators =
            operators.stream().map(o -> (PythonOperator) o).collect(Collectors.toList());
        operator = new ChainedPythonOperator(pythonOperators, configs);
      }
      // chained operator config is placed into `ChainedOperator`.
      mergedVertex =
          new JobVertex(
              headVertex.getVertexId(),
              headVertex.getParallelism(),
              headVertex.getVertexType(),
              operator,
              new HashMap<>());
    }

    mergedVertexMap.put(mergedVertex.getVertexId(), Pair.of(mergedVertex, verticesToMerge));
  }

  private List<JobEdge> createEdges() {
    List<JobEdge> edges = new ArrayList<>();
    mergedVertexMap.forEach(
        (id, pair) -> {
          JobVertex mergedVertex = pair.getLeft();
          List<JobVertex> mergedVertices = pair.getRight();
          JobVertex tailVertex = mergedVertices.get(mergedVertices.size() - 1);
          // input edge will be set up in input vertices
          if (outputEdgesMap.containsKey(tailVertex)) {
            outputEdgesMap
                .get(tailVertex)
                .forEach(
                    edge -> {
                      Pair<JobVertex, List<JobVertex>> downstreamPair =
                          mergedVertexMap.get(edge.getTargetVertexId());
                      // change ForwardPartition to RoundRobinPartition.
                      Partition partition = changePartition(edge.getPartition());
                      JobEdge newEdge =
                          new JobEdge(
                              mergedVertex.getVertexId(),
                              downstreamPair.getLeft().getVertexId(),
                              partition);
                      edges.add(newEdge);
                    });
          }
        });
    return edges;
  }

  /** Change ForwardPartition to RoundRobinPartition. */
  private Partition changePartition(Partition partition) {
    if (partition instanceof PythonPartition) {
      PythonPartition pythonPartition = (PythonPartition) partition;
      if (!pythonPartition.isConstructedFromBinary()
          && pythonPartition.getFunctionName().equals(PythonPartition.FORWARD_PARTITION_CLASS)) {
        return PythonPartition.RoundRobinPartition;
      } else {
        return partition;
      }
    } else {
      if (partition instanceof ForwardPartition) {
        return new RoundRobinPartition();
      } else {
        return partition;
      }
    }
  }

  private boolean canBeChained(
      JobVertex precedingVertex, JobVertex succeedingVertex, JobEdge edge) {
    if (jobGraph.getVertexOutputEdges(precedingVertex.getVertexId()).size() > 1
        || jobGraph.getVertexInputEdges(succeedingVertex.getVertexId()).size() > 1) {
      return false;
    }
    if (precedingVertex.getParallelism() != succeedingVertex.getParallelism()) {
      return false;
    }
    if (precedingVertex.getStreamOperator().getChainStrategy() == ChainStrategy.NEVER
        || succeedingVertex.getStreamOperator().getChainStrategy() == ChainStrategy.NEVER
        || succeedingVertex.getStreamOperator().getChainStrategy() == ChainStrategy.HEAD) {
      return false;
    }
    if (precedingVertex.getLanguage() != succeedingVertex.getLanguage()) {
      return false;
    }
    Partition partition = edge.getPartition();
    if (!(partition instanceof PythonPartition)) {
      return partition instanceof ForwardPartition;
    } else {
      PythonPartition pythonPartition = (PythonPartition) partition;
      return !pythonPartition.isConstructedFromBinary()
          && pythonPartition.getFunctionName().equals(PythonPartition.FORWARD_PARTITION_CLASS);
    }
  }
}
