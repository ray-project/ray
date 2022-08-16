import ray
from ray.dag import (
    DAGNode,
    InputNode,
)
from ray.serve._private.deployment_executor_node import DeploymentExecutorNode
from ray.serve._private.json_serde import dagnode_from_json
from ray.dag.utils import _DAGNodeNameGenerator
from ray.serve.handle import RayServeHandle

from functools import partial
from typing import Any, Dict, Tuple
from collections import defaultdict
import json
import time

import gradio as gr


async def _get_result(handle: RayServeHandle, node_uuid: str):
    """
    Returns the output of running the deployment on the DAGNode with uuid `node_uuid`
    """

    timeout_s = 20
    start = time.time()
    while time.time() - start < timeout_s:
        ref = await handle.get_object_ref_for_node.remote(node_uuid)
        if ref is not None:
            return await ref
    raise TimeoutError(f"Root DAG node isn't loaded after {timeout_s}s.")


class GraphVisualizer:
    def __init__(self):
        self._reset_state()

    def _reset_state(self):
        self.names: Dict[str, str] = {}
        self.depths: Dict[str, str] = defaultdict(lambda: 0)
        self.uuid_to_node: Dict[str, DAGNode] = {}
        # maps dag node uuid to unique instance of a gradio block
        self.node_to_block: Dict[str, Any] = {}
        # maps InputAttributeNodes to unique instance of interactive gradio block
        self.input_index_to_block: Dict[int, Any] = {}

    def _make_blocks(self):
        """
        Instantiates Gradio blocks for each graph node registered in self.depths.
        Nodes of depth 0 will be rendered in the top row, depth 1 in the second row,
        and so forth.
        """

        levels = {}
        for uuid in self.depths:
            levels.setdefault(self.depths[uuid], []).append(uuid)

        with gr.Row():
            for uuid in levels[0]:
                key = self.uuid_to_node[uuid]._key
                if key not in self.input_index_to_block:
                    self.input_index_to_block[key] = gr.Number(label=self.names[uuid])

        def render_level(n):
            for uuid in levels[n]:
                self.node_to_block[uuid] = gr.Number(
                    label=self.names[uuid], interactive=False
                )

        for level in range(1, max(levels.keys()) + 1):
            with gr.Row():
                render_level(level)

    def _get_depth_and_name(
        self,
        node: DAGNode,
        name_generator: _DAGNodeNameGenerator,
        nodes_to_exclude: Tuple,
    ):
        """
        Gets the name and depth of each graph node. Depth of each node is determined
        by the longest distance between that node and any InputAttributeNode. Nodes of
        any type in `nodes_to_exclude` will be skipped.
        """
        uuid = node.get_stable_uuid()
        for child_node in node._get_all_child_nodes():
            if not isinstance(child_node, nodes_to_exclude):
                self.depths[uuid] = max(
                    self.depths[uuid], self.depths[child_node.get_stable_uuid()] + 1
                )

        if not isinstance(node, nodes_to_exclude):
            self.names[uuid] = name_generator.get_node_name(node)
            self.uuid_to_node[uuid] = node
        return node

    def visualize_with_gradio(self, handle: RayServeHandle):
        """
        Launches a Gradio UI that allows interactive request dispatch and displays
        the evaluated outputs of each node in a deployment graph in real time.

        Args:
            handle: the handle to a deployment graph to be visualized with Gradio.
            Should be returned by a call to serve.run()
        """

        self._reset_state()

        # Load the root dag node from handle
        dag_node_json = ray.get(handle.get_dag_node_json.remote())
        dag = json.loads(dag_node_json, object_hook=dagnode_from_json)

        # Get name and level for each node in dag
        name_generator = _DAGNodeNameGenerator()
        dag.apply_recursive(
            lambda node: self._get_depth_and_name(
                node, name_generator, (InputNode, DeploymentExecutorNode)
            )
        )

        with gr.Blocks() as demo:
            self._make_blocks()

            submit = gr.Button("Submit")
            submit.click(
                fn=lambda *args: handle.predict.remote(args),
                inputs=list(self.input_index_to_block.values()),
                outputs=[],
            )
            for node_uuid, block in self.node_to_block.items():
                submit.click(partial(_get_result, handle, node_uuid), [], block)

        demo.launch()
