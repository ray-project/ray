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
from ray.serve._private.deployment_executor_node import DeploymentExecutorNode
from functools import partial
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
import pandas as pd
import PIL
import json

import gradio as gr
from ray.dag.utils import _DAGNodeNameGenerator


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
        # TODO(cindy): allow types of input to be passed into InputNode()
        if isinstance(node, InputAttributeNode):
            return gr.Number

        return_type = node.get_return_type()

        if return_type == 'int':
            return gr.Number
        elif return_type == 'str':
            return gr.Textbox
        elif return_type == 'DataFrame':
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
                        self.input_node_to_block[node_uuid] = block(label=names[node_uuid])
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
            visited[u.get_stable_uuid()] = True
            names[u.get_stable_uuid()] = self.name_generator.get_executor_node_name(u)
            self.uuid_to_node[u.get_stable_uuid()] = u

            for v in u._get_all_child_nodes():
                if isinstance(v, InputNode) or isinstance(v, DeploymentExecutorNode):
                    continue

                if visited[v.get_stable_uuid()] == False:
                    topologicalSortUtil(v)
                    depths[u.get_stable_uuid()] = max(
                        depths[u.get_stable_uuid()], 
                        depths[v.get_stable_uuid()] + 1
                    )

            stack.append(names[u.get_stable_uuid()])

        names = {}
        visited = defaultdict(bool)
        stack = []
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
        from ray.serve._private.json_serde import dagnode_from_json
        dag_node_json = ray.get(self.dag_handle.get_dag_node_json.remote())
        dag = json.loads(dag_node_json, object_hook=dagnode_from_json)

        with gr.Blocks() as demo:
            self.make_blocks(*self.top_sort_depth(dag))
            self.dfs_attach_change_events(dag)

            submit = gr.Button("Submit")

            input_blocks = list(self.input_node_to_block.values())
            output_blocks = [self.node_to_block[u] for u in self.input_node_to_block]
            submit.click(
                fn=self.submit_fn, # fn=partial(self.submit_fn, dag),
                inputs=input_blocks,
                outputs=output_blocks,
            )

        demo.launch()






    # NOT USED RIGHT NOW ------------------------------------------------------------

    def top_sort_depth_v2(self, dag) -> List[DAGNode]:
        # Top sort with root node having depth 0
        def topologicalSortUtil(u, d):
            visited[u.get_stable_uuid()] = True
            names[u.get_stable_uuid()] = self.name_generator.get_node_name(u)
            depths[u.get_stable_uuid()] = d

            for v in u._get_all_child_nodes():
                if isinstance(v, InputNode) or isinstance(v, ClassNode):
                    continue

                if visited[v.get_stable_uuid()] == False:
                    topologicalSortUtil(v, d + 1)
                else:
                    depths[v.get_stable_uuid()] = max(depths[v.get_stable_uuid()], d + 1)

            stack.append(names[u.get_stable_uuid()])

        names = {}
        visited = defaultdict(bool)
        stack = []
        depths = defaultdict(lambda: float("-inf"))
        topologicalSortUtil(dag, 0)
        return names, depths

    def top_sort_apply_recursive(self, dag: DAGNode):
        # Unfinished attempt at using apply_recursive to top sort + get depths of each node
        def f(node):
            names[node.get_stable_uuid()] = self.name_generator.get_node_name(node)
            stack.append(names[node.get_stable_uuid()])

        stack = []
        # depths = defaultdict(lambda: float('-inf'))
        names = {}
        dag.apply_recursive(f)

    def dfs_set_up_blocks(self, root):
        # Used before top sort approach
        with gr.Row():
            for u in root._get_all_child_nodes():
                # only generate blocks for certain types of DAGNode
                if isinstance(u, (InputNode, ClassNode)):
                    continue

                with gr.Column():
                    self.dfs_set_up_blocks(u)

        # only works for trees
        assert not root in self.node_to_block

        block_label = self.name_generator.get_node_name(root)
        if isinstance(root, InputAttributeNode):
            self.input_node_to_block[root.get_stable_uuid()] = gr.Number(label=block_label)
            self.node_to_block[root.get_stable_uuid()] = gr.Number(label=block_label, interactive=False, visible=False)
        else:
            self.node_to_block[root.get_stable_uuid()] = gr.Number(label=block_label, interactive=False)