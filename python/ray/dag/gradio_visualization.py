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
import time

import gradio as gr


async def _submit_fn(handle, node_uuid):
    timeout_s = 20
    start = time.time()
    while time.time() - start < timeout_s:
        ref = await handle.get_object_ref_for_node.remote(node_uuid)
        if ref is not None:
            return await ref
    raise TimeoutError(f"Root DAG node isn't loaded after {timeout_s}s.")


class GraphVisualizer:
    # maps dag nodes to unique instance of a gradio block
    node_to_block: Dict[DAGNode, Any]
    # maps InputAttributeNodes to unique instance of interactive gradio block
    input_node_to_block: Dict[DAGNode, Any]
    # maps an uuid to instance of DAGNode
    uuid_to_node: Dict[str, DAGNode]

    def __init__(self):
        self.name_generator = _DAGNodeNameGenerator()

    def reset_state(self):
        self.node_to_block = {}
        self.input_node_to_block = {}
        self.uuid_to_node = {}
        self.names = {}
        self.depths = defaultdict(lambda: 0)

    def make_blocks(self):
        max_depth = max(set(self.depths.values()))

        def render_level(level):
            for node_uuid, v in self.depths.items():
                if v != level:
                    continue

                node = self.uuid_to_node[node_uuid]
                if isinstance(node, InputAttributeNode):
                    self.input_node_to_block[node_uuid] = gr.Number(
                        label=self.names[node_uuid]
                    )
                else:
                    self.node_to_block[node_uuid] = gr.Number(
                        label=self.names[node_uuid], interactive=False
                    )

        for level in range(max_depth + 1):
            with gr.Row():
                render_level(level)

    def get_depth_and_name(self, node: DAGNode):
        if isinstance(node, (InputNode, DeploymentExecutorNode)):
            return node

        uuid = node.get_stable_uuid()
        self.uuid_to_node[uuid] = node
        self.names[uuid] = self.name_generator.get_node_name(node)

        for child_node in node._get_all_child_nodes():
            if not isinstance(child_node, (InputNode, DeploymentExecutorNode)):
                self.depths[uuid] = max(
                    self.depths[uuid], self.depths[child_node.get_stable_uuid()] + 1
                )

        return node

    def visualize_with_gradio(self, handle):
        self.reset_state()

        # Load the root dag node from handle
        dag_node_json = ray.get(handle.get_dag_node_json.remote())
        dag = json.loads(dag_node_json, object_hook=dagnode_from_json)
        dag.apply_recursive(lambda node: self.get_depth_and_name(node))

        with gr.Blocks() as demo:
            self.make_blocks()

            submit = gr.Button("Submit")
            submit.click(
                fn=lambda *args: handle.predict.remote(args),
                inputs=list(self.input_node_to_block.values()),
                outputs=[],
            )
            for node, block in self.node_to_block.items():
                submit.click(partial(_submit_fn, handle, node), [], block)

        demo.launch()
