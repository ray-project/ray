import ray
from ray.dag import (
    DAGNode,
    InputNode,
    InputAttributeNode,
    ClassNode,
)
from ray.serve._private.deployment_executor_node import DeploymentExecutorNode
from ray.serve._private.json_serde import dagnode_from_json
from ray.dag.utils import _DAGNodeNameGenerator

from functools import partial
from typing import Any, Dict, List
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

        if return_type == "int":
            return gr.Number
        elif return_type == "str":
            return gr.Textbox
        elif return_type == "DataFrame":
            return gr.Dataframe
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

    def top_sort_depth(self, dag) -> List[DAGNode]:
        def topologicalSortUtil(u):
            uuid = u.get_stable_uuid()
            visited[uuid] = True
            names[uuid] = self.name_generator.get_executor_node_name(u)
            self.uuid_to_node[uuid] = u

            for v in u._get_all_child_nodes():
                if isinstance(v, InputNode) or isinstance(v, DeploymentExecutorNode):
                    continue

                child_uuid = v.get_stable_uuid()
                if visited[child_uuid] is False:
                    topologicalSortUtil(v)
                    depths[uuid] = max(depths[uuid], depths[child_uuid] + 1)

        names = {}
        visited = defaultdict(bool)
        depths = defaultdict(lambda: 0)
        topologicalSortUtil(dag)
        return names, depths

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

        with gr.Blocks() as demo:
            self.make_blocks(*self.top_sort_depth(dag))
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
