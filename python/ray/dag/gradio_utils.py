import ray
from ray.dag import DAGNode
from ray.dag import (
    DAGNode,
    InputNode,
    InputAttributeNode,
    FunctionNode,
    ClassNode,
    ClassMethodNode,
)
from functools import partial
from typing import Any, Dict, List, Optional, Callable

import gradio as gr
from ray.dag.utils import _DAGNodeNameGenerator


class GraphVisualizer:
    # maps dag nodes to unique instance of a gradio block
    node_to_block: Dict[DAGNode, Any]

    # nodes that are directly connected to InputAttributeNodes
    # corresponding blocks are updated when submit button is pressed
    level1_nodes: List[DAGNode]

    def __init__(self):
        self.name_generator = _DAGNodeNameGenerator()

    def update_block(self, node, *args):
        return ray.get(self.cache[node.get_stable_uuid()])

    def dfs_set_up_blocks(self, root):
        with gr.Row():
            for node in root._get_all_child_nodes():
                # only generate blocks for certain types of DAGNode
                if isinstance(node, (InputNode, ClassNode)):
                    continue

                # TODO: a more robust check for what qualifies as a level 1 node
                if not isinstance(root, FunctionNode) and isinstance(
                    node, InputAttributeNode
                ):
                    self.level1_nodes.append(root)

                with gr.Column():
                    self.dfs_set_up_blocks(node)

        # currently only works for trees
        # TODO: add functionality for dags that are not trees
        assert not root in self.node_to_block

        block_label = self.name_generator.get_node_name(root)
        if isinstance(root, InputAttributeNode):
            self.node_to_block[root] = gr.Number(label=block_label)
        else:
            self.node_to_block[root] = gr.Textbox(label=block_label, interactive=False)

    def dfs_attach_change_events(self, root):
        for node in root._get_all_child_nodes():
            if isinstance(node, InputNode) or isinstance(node, ClassNode):
                continue
            self.dfs_attach_change_events(node)

            # if node is not level 1, then attach change event to node
            # (instead of updating when submit button is pressed, just propagate changes)
            # TODO: make more robust
            if not isinstance(node, InputAttributeNode):
                self.node_to_block[node].change(
                    fn=partial(self.update_block, root),
                    inputs=self.node_to_block[node],
                    outputs=self.node_to_block[root],
                )

    def submit_fn(self, dag, *args):
        self.ref, self.cache = dag.execute(*args)
        return [
            ray.get(self.cache[node.get_stable_uuid()]) for node in self.level1_nodes
        ]

    def visualize_with_gradio(self, dag: DAGNode):
        self.node_to_block = {}
        self.level1_nodes = []

        with gr.Blocks() as demo:
            self.dfs_set_up_blocks(dag)
            self.dfs_attach_change_events(dag)

            submit = gr.Button("Submit")

            input_blocks = [
                block
                for (node, block) in self.node_to_block.items()
                if isinstance(node, InputAttributeNode)
            ]
            level1_blocks = [self.node_to_block[node] for node in self.level1_nodes]
            submit.click(
                fn=partial(self.submit_fn, dag),
                inputs=input_blocks,
                outputs=level1_blocks,
            )

        demo.launch()
