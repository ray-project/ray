import logging
from collections import Counter
from typing import List, Union

import onnx

import onnxslim.third_party.onnx_graphsurgeon as gs
from onnxslim.core.pattern import get_node_feeds
from onnxslim.core.pattern.registry import get_fusion_patterns
from onnxslim.third_party.onnx_graphsurgeon.ir.graph import Graph

logger = logging.getLogger("onnxslim")

from .dead_node_elimination import dead_node_elimination
from .subexpression_elimination import subexpression_elimination
from .weight_tying import tie_weights


def optimize_model(model: Union[onnx.ModelProto, gs.Graph], skip_fusion_patterns: str = None) -> onnx.ModelProto:
    """Optimize and transform the given ONNX model using various fusion patterns and graph rewriting techniques."""
    graph = model if isinstance(model, gs.Graph) else gs.import_onnx(model)
    fusion_patterns = get_fusion_patterns(skip_fusion_patterns)
    fusion_pairs = find_matches(graph, fusion_patterns)
    for match in fusion_pairs.values():
        graph.replace_custom_layer(**match)
    graph.cleanup(remove_unused_graph_inputs=True).toposort()
    dead_node_elimination(graph)
    graph.cleanup(remove_unused_graph_inputs=True).toposort()
    subexpression_elimination(graph)
    graph.cleanup(remove_unused_graph_inputs=True).toposort()
    tie_weights(graph)
    model = gs.export_onnx(graph)

    return model


@gs.Graph.register()
def replace_custom_layer(
    self,
    op: str,
    inputs,
    outputs: List[str],
    name: str,
    attrs: dict = None,
    domain: str = "ai.onnx.contrib",
):
    """Replace a custom layer in the computational graph with specified parameters and domain."""
    return self.layer(
        op=op,
        inputs=inputs,
        outputs=outputs,
        name=name,
        attrs=attrs,
        domain=domain,
    )


def find_matches(graph: Graph, fusion_patterns: dict):
    """Find matching patterns in the graph based on provided fusion patterns."""
    match_map = {}
    counter = Counter()
    for node in reversed(graph.nodes):
        if node.name not in match_map:
            for layer_type, pattern_matcher in fusion_patterns.items():
                match = pattern_matcher.match(node)
                if match:
                    match_case = pattern_matcher.rewrite(opset=graph.opset)
                    logger.debug(f"matched pattern {layer_type}")
                    for _, match in match_case.items():
                        if "op" not in match:
                            match.update({"op": layer_type})
                        if "name" not in match:
                            match.update({"name": f"{layer_type.lower()}_{counter[layer_type]}"})
                        counter.update([layer_type])
                    match_map.update(match_case)

    return match_map


def get_previous_node_by_type(node, op_type, trajectory=None):
    """Recursively find and return the first preceding node of a specified type in the computation graph."""
    if trajectory is None:
        trajectory = []
    node_feeds = get_node_feeds(node)
    for node_feed in node_feeds:
        trajectory.append(node_feed)
        if node_feed.op == op_type:
            return trajectory
        else:
            return get_previous_node_by_type(node_feed, op_type, trajectory)
