import ray
from ray.dag import (
    DAGNode,
    InputAttributeNode,
)
from ray.serve._private.deployment_function_executor_node import (
    DeploymentFunctionExecutorNode,
)
from ray.serve._private.deployment_method_executor_node import (
    DeploymentMethodExecutorNode,
)
from ray.serve._private.json_serde import dagnode_from_json
from ray.dag.utils import _DAGNodeNameGenerator
from ray.serve.handle import RayServeHandle

from typing import Any, Dict, Optional
from collections import defaultdict
import json
import asyncio
import logging


logger = logging.getLogger(__name__)
_gradio = None


def lazy_import_gradio():
    global _gradio
    if _gradio is None:
        try:
            import gradio
        except ModuleNotFoundError:
            print(
                "Gradio isn't installed. Run `pip install gradio` to use Gradio to "
                "visualize a Serve deployment graph."
            )
            raise

        _gradio = gradio
    return _gradio


class GraphVisualizer:
    def __init__(self):
        lazy_import_gradio()
        self._reset_state()

    def _reset_state(self):
        """Resets state for each new RayServeHandle representing a new DAG."""
        self.cache = {}
        self.resolved_nodes = 0
        self.finished_last_inference = True

        # maps DAGNode uuid to a DAGNode instance with that uuid
        self.uuid_to_node: Dict[str, DAGNode] = {}
        # maps DAGNode uuid to unique instance of a gradio block
        self.uuid_to_block: Dict[str, Any] = {}
        # maps InputAttributeNodes to unique instance of interactive gradio block
        self.input_index_to_block: Dict[int, Any] = {}

    def clear_cache(self):
        self.cache = {}

    def _make_blocks(self, depths: Dict[str, int]):
        """Instantiates Gradio blocks for each graph node stored in depths.
        Nodes of depth 1 will be rendered in the top row, depth 2 in the second row,
        and so forth.

        Args:
            depths: maps uuids of nodes in the DAG to their depth
        """
        gr = lazy_import_gradio()

        levels = {}
        for uuid in depths:
            if isinstance(
                self.uuid_to_node[uuid],
                (
                    InputAttributeNode,
                    DeploymentMethodExecutorNode,
                    DeploymentFunctionExecutorNode,
                ),
            ):
                levels.setdefault(depths[uuid], []).append(uuid)

        name_generator = _DAGNodeNameGenerator()

        def render_level(level):
            for uuid in levels[level]:
                node = self.uuid_to_node[uuid]
                name = name_generator.get_node_name(node)

                # InputAttributNodes should have level 1
                # Because the InputNode has level 0 but is not rendered
                if level == 1:
                    key = node._key
                    if key not in self.input_index_to_block:
                        self.input_index_to_block[key] = gr.Number(label=name)
                else:
                    self.uuid_to_block[uuid] = gr.Number(label=name, interactive=False)

        for level in sorted(levels.keys()):
            with gr.Row():
                render_level(level)

    def _fetch_depths(self, node: DAGNode, depths: Dict[str, int]) -> DAGNode:
        """Gets the depth of a graph node, which is determined by the longest distance
        between that node and any InputAttributeNode. The single InputNode in the graph
        will have depth 0, and all InputAttributeNodes will have depth 1.

        Args:
            node: the graph node to process
            depths: map between DAGNode uuid to the current longest found distance
                between the DAGNode and any InputAttributeNode

        Returns:
            The original node.
        """
        uuid = node.get_stable_uuid()
        for child_node in node._get_all_child_nodes():
            depths[uuid] = max(depths[uuid], depths[child_node.get_stable_uuid()] + 1)

        self.uuid_to_node[uuid] = node
        return node

    async def _get_result(self, node_uuid: str):
        """Retrieves the execution output of the inputted DAGNode, from last execution.

        This function should only be called after a request has been sent through
        self._send_request() separately.
        """
        result = await self.cache[node_uuid]
        self.resolved_nodes += 1
        if self.resolved_nodes == len(self.uuid_to_block):
            self.finished_last_inference = True
        return result

    async def _send_request(self, trigger_value, *args):
        """Sends a request to the root DAG node through self.handle and retrieves the
        cached object refs pointing to return values of each executed node in the DAG.

        Will not run if the last inference process has not finished (if all nodes in
        DAG have been resolved).
        """
        if not self.finished_last_inference:
            logger.warning("Last inference has not finished yet.")
            return trigger_value

        self.handle.predict.remote(args, _cache_refs=True)
        self.cache = await self.handle.get_intermediate_object_refs.remote()

        # Set state to track the inference process
        self.resolved_nodes = 0
        self.finished_last_inference = False

        return trigger_value + 1

    def visualize_with_gradio(
        self,
        driver_handle: RayServeHandle,
        port: Optional[int] = None,
        _launch: bool = True,
        _block: bool = True,
    ):
        """Launches a Gradio UI that allows interactive request dispatch and displays
        the evaluated outputs of each node in a deployment graph in real time.

        Args:
            driver_handle: The handle to a DAGDriver deployment obtained through a call
                to serve.run(). The DAG rooted at that DAGDriver deployment will be
                visualized through Gradio.
            port: The port on which to start the Gradio app. If None, will default to
                Gradio's default.
            _launch: Whether to launch the Gradio app. Used for unit testing purposes.
            _block: Whether to block the main thread while the Gradio server is running.
                Used for unit testing purposes.
        """
        gr = lazy_import_gradio()

        self._reset_state()
        self.handle = driver_handle

        # Load the root DAG node from handle
        dag_node_json = ray.get(self.handle.get_dag_node_json.remote())
        self.dag = json.loads(dag_node_json, object_hook=dagnode_from_json)

        # Get level for each node in dag
        depths = defaultdict(lambda: 0)
        self.dag.apply_recursive(lambda node: self._fetch_depths(node, depths))

        with gr.Blocks() as demo:
            self._make_blocks(depths)

            with gr.Row():
                submit = gr.Button("Run").style()
                trigger = gr.Number(visible=False)
                clear = gr.Button("Clear").style()

            # Add event listener that sends the request to the deployment graph
            submit.click(
                fn=self._send_request,
                inputs=[trigger] + list(self.input_index_to_block.values()),
                outputs=trigger,
            )
            # Add event listeners that resolve object refs for each of the nodes
            for node_uuid, block in self.uuid_to_block.items():
                trigger.change(self._get_result, gr.Variable(node_uuid), block)

            # Resets all blocks if Clear button is clicked
            all_blocks = [*self.uuid_to_block.values()] + [
                *self.input_index_to_block.values()
            ]
            clear.click(
                lambda: self.clear_cache() or [None] * len(all_blocks), [], all_blocks
            )

        if _launch:
            return demo.launch(server_port=port, prevent_thread_lock=not _block)
