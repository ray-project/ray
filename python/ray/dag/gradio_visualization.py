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
from typing import Any, Dict
from collections import defaultdict
import json
import time

import gradio as gr


class GraphVisualizer:
    def __init__(self):
        self._reset_state()

    def _reset_state(self):
        # maps dag node uuid to a DAGNode instance with that uuid
        self.uuid_to_node: Dict[str, DAGNode] = {}
        # maps dag node uuid to unique instance of a gradio block
        self.node_to_block: Dict[str, Any] = {}
        # maps InputAttributeNodes to unique instance of interactive gradio block
        self.input_index_to_block: Dict[int, Any] = {}

    def _make_blocks(self, depths):
        """
        Instantiates Gradio blocks for each graph node registered in self.depths.
        Nodes of depth 0 will be rendered in the top row, depth 1 in the second row,
        and so forth.
        """

        levels = {}
        for uuid in depths:
            if not isinstance(
                self.uuid_to_node[uuid], (InputNode, DeploymentExecutorNode)
            ):
                levels.setdefault(depths[uuid], []).append(uuid)

        name_generator = _DAGNodeNameGenerator()

        def render_level(n):
            for uuid in levels[n]:
                node = self.uuid_to_node[uuid]
                name = name_generator.get_node_name(node)

                # InputAttributNodes should have level 1
                if n == 1:
                    key = node._key
                    if key not in self.input_index_to_block:
                        self.input_index_to_block[key] = gr.Number(label=name)
                else:
                    self.node_to_block[uuid] = gr.Number(label=name, interactive=False)

        for level in sorted(levels.keys()):
            with gr.Row():
                render_level(level)

    def _fetch_depths(self, node: DAGNode, depths: Dict[str, int]):
        """
        Gets the depth of each graph node, which is determined by the longest distance
        between that node and any InputAttributeNode. The single InputNode in the graph
        will have depth 0, and all InputAttributeNodes will have depth 1.
        """
        uuid = node.get_stable_uuid()
        for child_node in node._get_all_child_nodes():
            depths[uuid] = max(depths[uuid], depths[child_node.get_stable_uuid()] + 1)

        self.uuid_to_node[uuid] = node
        return node

    async def _get_result(self, node_uuid: str):
        """
        Returns the output of running the deployment on DAGNode with uuid `node_uuid`
        """

        timeout_s = 20
        start = time.time()
        while time.time() - start < timeout_s:
            ref = await self.handle.get_object_ref_for_node.remote(node_uuid)
            if ref is not None:
                return await ref
        raise TimeoutError(f"Fetching node output timed out after {timeout_s}s.")

    def visualize_with_gradio(self, handle: RayServeHandle):
        """
        Launches a Gradio UI that allows interactive request dispatch and displays
        the evaluated outputs of each node in a deployment graph in real time.

        Args:
            handle: the handle to a deployment graph to be visualized with Gradio.
            Should be returned by a call to serve.run()
        """

        self._reset_state()
        self.handle = handle

        # Load the root dag node from handle
        dag_node_json = ray.get(handle.get_dag_node_json.remote())
        dag = json.loads(dag_node_json, object_hook=dagnode_from_json)

        # Get name and level for each node in dag
        depths = defaultdict(lambda: 0)
        dag.apply_recursive(lambda node: self._fetch_depths(node, depths))

        # Wraps _get_result because functools.partial doesn't work with asynchronous
        # class methods: https://stackoverflow.com/q/67020609/11162437
        async def get_result_wrapper(node_uuid):
            return await self._get_result(node_uuid)

        with gr.Blocks() as demo:
            self._make_blocks(depths)

            submit = gr.Button("Submit")
            # Add event listener that sends the request to the deployment graph
            submit.click(
                fn=lambda *args: self.handle.predict.remote(args),
                inputs=list(self.input_index_to_block.values()),
                outputs=[],
            )
            # Add event listeners that resolve object refs for each of the nodes
            for node_uuid, block in self.node_to_block.items():
                submit.click(partial(get_result_wrapper, node_uuid), [], block)

        demo.launch()
