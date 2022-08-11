import ray
from ray.dag import (
    DAGNode,
    InputNode,
    InputAttributeNode,
)
from ray.serve._private.deployment_executor_node import DeploymentExecutorNode
from ray.serve._private.json_serde import dagnode_from_json
from ray.dag.utils import _DAGNodeNameGenerator

from functools import partial
from typing import Any, Dict
from collections import defaultdict
import json

import gradio as gr


class GraphVisualizer:
    # maps dag nodes to unique instance of a gradio block
    node_to_block: Dict[DAGNode, Any]
    # maps InputAttributeNodes to unique instance of interactive gradio block
    input_node_to_block: Dict[DAGNode, Any]
    # maps an uuid to instance of DAGNode
    uuid_to_node: Dict[str, DAGNode]

    def __init__(self):
        self.name_generator = _DAGNodeNameGenerator()
        self.count = 0

    def block_type(self, node):
        return_type = node.get_return_type()

        if return_type == "<class 'int'>":
            return gr.Number
        elif return_type == "<class 'str'>":
            return gr.Textbox
        elif return_type == "<class 'bool'>":
            return gr.Checkbox
        elif return_type == "<class 'pandas.core.frame.DataFrame'>":
            return gr.Dataframe
        elif (
            return_type == "<class 'list'>"
            or return_type == "<class 'dict'>"
            or return_type == "<class 'numpy.ndarray'>"
            or return_type == "typing.List"
            or return_type == "typing.Dict"
        ):
            return gr.JSON
        elif return_type == "<class 'PIL.ImageFile.ImageFile'>":
            return gr.Image
        elif return_type == "typing.Tuple[int, int]":
            return gr.Audio
        return gr.Textbox

    def update_block(self, u, *args):
        return ray.get(self.cache[u.get_stable_uuid()])

    def dfs_attach_change_events(self, root):
        for u in root._get_all_child_nodes():
            if isinstance(u, InputNode) or isinstance(u, DeploymentExecutorNode):
                continue
            self.dfs_attach_change_events(u)

            self.node_to_block[u.get_stable_uuid()].change(
                fn=partial(self.update_block, root),
                inputs=self.node_to_block[u.get_stable_uuid()],
                outputs=self.node_to_block[root.get_stable_uuid()],
            )

    def make_blocks(self, names, depths):
        max_depth = max(set(depths.values()))

        def render_depth(depth):
            for node_uuid, v in depths.items():
                node = self.uuid_to_node[node_uuid]
                if v == depth:
                    block = self.block_type(node)
                    if isinstance(node, InputAttributeNode):
                        self.input_node_to_block[node_uuid] = block(
                            label=names[node_uuid]
                        )
                        self.node_to_block[node_uuid] = block(
                            label=names[node_uuid], interactive=False, visible=False
                        )
                    else:
                        self.node_to_block[node_uuid] = block(
                            label=names[node_uuid], interactive=False
                        )

        # for depth in range(max_depth + 1, -1, -1):
        for depth in range(max_depth + 1):
            with gr.Row():
                render_depth(depth)

    def get_depth_and_name(self, node: DAGNode):
        if isinstance(node, (InputNode, DeploymentExecutorNode)):
            return node

        uuid = node.get_stable_uuid()

        # getting name
        self.names[uuid] = self.name_generator.get_node_name(node)
        self.uuid_to_node[uuid] = node

        # getting depth
        for child_node in node._get_all_child_nodes():
            if not isinstance(child_node, (InputNode, DeploymentExecutorNode)):
                self.depths[uuid] = max(
                    self.depths[uuid], self.depths[child_node.get_stable_uuid()] + 1
                )

        return node

    def submit_fn(self, *args):
        self.dag_handle.predict.remote(args)
        self.cache = ray.get(self.dag_handle.get_intermediate_node_object_refs.remote())
        return args

    def visualize_with_gradio(self, dag_handle):
        self.node_to_block = {}
        self.input_node_to_block = {}
        self.uuid_to_node = {}

        self.dag_handle = dag_handle

        # Load the root dag node from dag_handle
        dag_node_json = ray.get(self.dag_handle.get_dag_node_json.remote())
        dag = json.loads(dag_node_json, object_hook=dagnode_from_json)

        self.names = {}
        self.depths = defaultdict(lambda: 0)
        dag.apply_recursive(lambda node: self.get_depth_and_name(node))

        with gr.Blocks() as demo:
            self.make_blocks(self.names, self.depths)
            self.dfs_attach_change_events(dag)

            submit = gr.Button("Submit")

            input_blocks = list(self.input_node_to_block.values())
            output_blocks = [self.node_to_block[u] for u in self.input_node_to_block]
            submit.click(
                fn=self.submit_fn,
                inputs=input_blocks,
                outputs=output_blocks,
            )

        demo.launch()
