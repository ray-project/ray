# coding: utf-8
import os
import sys

import pytest

from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode, ClassMethodNode
from ray.dag.dag_node_operation import (
    _DAGNodeOperationType,
    _DAGOperationGraphNode,
    _DAGNodeOperation,
    _select_next_nodes,
    _build_dag_node_operation_graph,
    _add_edge,
)
from ray.dag.compiled_dag_node import CompiledTask
from typing import List, Dict, Tuple
from ray.actor import ActorHandle

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


def mock_actor_handle_init(self, actor_id: str):
    self._ray_actor_id = actor_id


def mock_init(self):
    pass


def generate_dag_graph_nodes(local_idx, dag_idx, actor_handle, requires_nccl):
    graph_nodes = {}
    for op_type in _DAGNodeOperationType:
        graph_nodes[op_type] = _DAGOperationGraphNode(
            _DAGNodeOperation(local_idx, op_type),
            dag_idx,
            actor_handle,
            requires_nccl,
        )
    return graph_nodes


class TestSelectNextNodes:
    """
    Test whether `_select_next_nodes` function selects the next nodes for
    topological sort to generate execution schedule correctly.

    dag_idx: Each DAG node has a unique global index.
    local_idx: The DAG node's index in the actor's `executable_tasks` list.
    """

    def test_two_candidates_on_same_actor(self):
        """
        Simulate the case where there are two candidates on the same actor.
        The candidate with the smaller index in the `executable_tasks` list
        should be selected.

        driver -> fake_actor.op -> fake_actor.op -> driver

        In the example above, both READ operations on the fake_actor have zero
        in-degree. The operation with the smaller index in the executable_tasks
        list should be selected first; therefore, the one on the left side will
        be selected first.
        """
        fake_actor = "fake_actor"
        # The DAG node has a global index of 1, and its index in the
        # actor's `executable_tasks` list is 0.
        dag_idx_1 = 1
        dag_node_1 = _DAGOperationGraphNode(
            _DAGNodeOperation(0, _DAGNodeOperationType.READ),
            dag_idx_1,
            fake_actor,
            False,
        )
        # The DAG node has a global index of 2, and its index in the
        # actor's `executable_tasks` list is 1.
        dag_idx_2 = 2
        dag_node_2 = _DAGOperationGraphNode(
            _DAGNodeOperation(1, _DAGNodeOperationType.READ),
            dag_idx_2,
            fake_actor,
            False,
        )
        mock_actor_to_candidates = {
            fake_actor: [
                dag_node_1,
                dag_node_2,
            ],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, None)
        assert len(next_nodes) == 1
        assert next_nodes[0] == dag_node_1

    def test_only_one_nccl_write(self, monkeypatch):
        """
        Simulate the case where there is only one candidate which is a NCCL
        WRITE operation. In this case, `_select_next_nodes` should return both
        the NCCL WRITE operation and the corresponding READ operation.

        driver -> fake_actor_1.op -> fake_actor_2.op -> driver

        In the example above, communication between fake_actor_1 and fake_actor_2
        is done using NCCL. The following test case simulates a scenario where the
        READ and COMPUTE operations on fake_actor_1 have already been added to the
        execution schedule.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)
        fake_actor_1, dag_idx_1, local_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, dag_idx_2, local_idx_2 = ActorHandle("fake_actor_2"), 2, 0
        mock_graph = {
            dag_idx_1: generate_dag_graph_nodes(
                local_idx_1, dag_idx_1, fake_actor_1, True
            ),
            dag_idx_2: generate_dag_graph_nodes(
                local_idx_2, dag_idx_2, fake_actor_2, False
            ),
        }
        del mock_graph[dag_idx_1][_DAGNodeOperationType.READ]
        del mock_graph[dag_idx_1][_DAGNodeOperationType.COMPUTE]

        _add_edge(
            mock_graph[dag_idx_1][_DAGNodeOperationType.WRITE],
            mock_graph[dag_idx_2][_DAGNodeOperationType.READ],
        )
        _add_edge(
            mock_graph[dag_idx_2][_DAGNodeOperationType.READ],
            mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE],
        )
        _add_edge(
            mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE],
            mock_graph[dag_idx_2][_DAGNodeOperationType.WRITE],
        )
        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[dag_idx_1][_DAGNodeOperationType.WRITE]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert len(next_nodes) == 2
        assert next_nodes[0] == mock_graph[dag_idx_1][_DAGNodeOperationType.WRITE]
        assert next_nodes[1] == mock_graph[dag_idx_2][_DAGNodeOperationType.READ]

    def test_two_nccl_writes(self, monkeypatch):
        """
        Simulate a scenario where there are two candidates that are NCCL WRITE
        operations. In this case, _select_next_nodes can choose either of the
        two NCCL WRITE operations and their corresponding READ operations.

        driver -> fake_actor_1.op -> fake_actor_2.op -> driver
               |                                     |
               -> fake_actor_2.op -> fake_actor_1.op -

        In the example above, communication between fake_actor_1 and fake_actor_2 is
        done using NCCL. The following test case simulates a scenario where the READ
        and COMPUTE operations on both the DAG nodes with smaller bind_index on
        fake_actor_1 and fake_actor_2 have already been added to the execution schedule.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        dag_idx_1_0, local_idx_1_0 = 1, 0
        dag_idx_1_1, local_idx_1_1 = 3, 1
        fake_actor_2 = ActorHandle("fake_actor_2")
        dag_idx_2_0, local_idx_2_0 = 2, 0
        dag_idx_2_1, local_idx_2_1 = 4, 1

        # Run the test 10 times to ensure that the result of `_select_next_nodes`
        # is deterministic.
        for _ in range(20):
            mock_graph = {
                dag_idx_1_0: generate_dag_graph_nodes(
                    local_idx_1_0, dag_idx_1_0, fake_actor_1, True
                ),
                dag_idx_1_1: generate_dag_graph_nodes(
                    local_idx_1_1, dag_idx_1_1, fake_actor_1, False
                ),
                dag_idx_2_0: generate_dag_graph_nodes(
                    local_idx_2_0, dag_idx_2_0, fake_actor_2, True
                ),
                dag_idx_2_1: generate_dag_graph_nodes(
                    local_idx_2_1, dag_idx_2_1, fake_actor_2, False
                ),
            }
            del mock_graph[dag_idx_1_0][_DAGNodeOperationType.READ]
            del mock_graph[dag_idx_1_0][_DAGNodeOperationType.COMPUTE]
            del mock_graph[dag_idx_2_0][_DAGNodeOperationType.READ]
            del mock_graph[dag_idx_2_0][_DAGNodeOperationType.COMPUTE]

            _add_edge(
                mock_graph[dag_idx_1_0][_DAGNodeOperationType.WRITE],
                mock_graph[dag_idx_2_1][_DAGNodeOperationType.READ],
            )
            _add_edge(
                mock_graph[dag_idx_2_0][_DAGNodeOperationType.WRITE],
                mock_graph[dag_idx_1_1][_DAGNodeOperationType.READ],
            )
            _add_edge(
                mock_graph[dag_idx_2_1][_DAGNodeOperationType.READ],
                mock_graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE],
            )
            _add_edge(
                mock_graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE],
                mock_graph[dag_idx_2_1][_DAGNodeOperationType.WRITE],
            )
            _add_edge(
                mock_graph[dag_idx_1_1][_DAGNodeOperationType.READ],
                mock_graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE],
            )
            _add_edge(
                mock_graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE],
                mock_graph[dag_idx_1_1][_DAGNodeOperationType.WRITE],
            )
            mock_actor_to_candidates = {
                fake_actor_1: [mock_graph[dag_idx_1_0][_DAGNodeOperationType.WRITE]],
                fake_actor_2: [mock_graph[dag_idx_2_0][_DAGNodeOperationType.WRITE]],
            }

            next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
            assert len(next_nodes) == 2
            assert next_nodes[0] == mock_graph[dag_idx_1_0][_DAGNodeOperationType.WRITE]
            assert next_nodes[1] == mock_graph[dag_idx_2_1][_DAGNodeOperationType.READ]


class TestBuildDAGNodeOperationGraph:
    """
    Test whether `_build_dag_node_operation_graph` function adds the correct
    edges between the nodes in the operation graph base on the 3 rules mentioned
    in the doc string of `_build_dag_node_operation_graph`.
    """

    def check_edges_between_read_compute_write(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        dag_idx: int,
        expected_num_edges: List[Tuple[int, int]],
    ):
        """
        Check whether edges from READ to COMPUTE, and from COMPUTE to WRITE,
        belonging to the same task are added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            dag_idx: The global index of the task used to access the task in
                `idx_to_task`.
            expected_num_edges: A list of tuples where each tuple contains the expected
                number of in-edges and out-edges for READ, COMPUTE, and WRITE
                operations.
        """
        assert len(expected_num_edges) == 3
        assert len(graph[dag_idx]) == 3
        read_node = graph[dag_idx][_DAGNodeOperationType.READ]
        compute_node = graph[dag_idx][_DAGNodeOperationType.COMPUTE]
        write_node = graph[dag_idx][_DAGNodeOperationType.WRITE]

        for idx, node in enumerate([read_node, compute_node, write_node]):
            assert node.in_degree == expected_num_edges[idx][0]
            assert len(node.out_edges) == expected_num_edges[idx][1]

        assert (dag_idx, _DAGNodeOperationType.COMPUTE) in read_node.out_edges
        assert (dag_idx, _DAGNodeOperationType.READ) in compute_node.in_edges
        assert (dag_idx, _DAGNodeOperationType.WRITE) in compute_node.out_edges
        assert (dag_idx, _DAGNodeOperationType.COMPUTE) in write_node.in_edges

    def check_edge_between_writer_and_reader(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        writer_dag_idx: int,
        reader_dag_idx: int,
    ):
        """
        Check whether the edge from writer's WRITE to reader's READ operation is added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            writer_dag_idx: The index of the task used to access the task
                that the writer belongs to in `idx_to_task`.
            reader_dag_idx: The index of the task used to access the task
                that the reader belongs to in `idx_to_task`.
        """
        write_node = graph[writer_dag_idx][_DAGNodeOperationType.WRITE]
        read_node = graph[reader_dag_idx][_DAGNodeOperationType.READ]

        assert (reader_dag_idx, _DAGNodeOperationType.READ) in write_node.out_edges
        assert (writer_dag_idx, _DAGNodeOperationType.WRITE) in read_node.in_edges

    def check_edge_between_compute_nodes(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        dag_idx_1: int,
        dag_idx_2: int,
    ):
        """
        Check whether the edge from COMPUTE with `bind_index` i to COMPUTE with
            `bind_index` i+1 if they belong to the same actor.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            dag_idx_1: The index of the task used to access the task in
                `idx_to_task`.
            dag_idx_2: The index of the task used to access the task in
                `idx_to_task`. Note that both tasks belong to the same actor, and the
                `bind_index` of the second task is equal to the `bind_index` of the
                first task plus one.
        """
        compute_node_1 = graph[dag_idx_1][_DAGNodeOperationType.COMPUTE]
        compute_node_2 = graph[dag_idx_2][_DAGNodeOperationType.COMPUTE]

        assert (dag_idx_2, _DAGNodeOperationType.COMPUTE) in compute_node_1.out_edges
        assert (dag_idx_1, _DAGNodeOperationType.COMPUTE) in compute_node_2.in_edges

    def test_edges_between_read_compute_write(self, monkeypatch):
        """
        driver -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds edges
        between READ/COMPUTE and COMPUTE/WRITE operations on the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, MultiOutputNode()),
        }

        fake_actor = "fake_actor"
        dag_idx = 1
        actor_to_operation_nodes = {
            fake_actor: [
                list(generate_dag_graph_nodes(0, dag_idx, fake_actor, False).values())
            ]
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 1

        self.check_edges_between_read_compute_write(
            graph, dag_idx, [(0, 1), (1, 1), (1, 0)]
        )

    def test_edge_between_writer_and_reader(self, monkeypatch):
        """
        driver -> fake_actor_1.op -> fake_actor_2.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the writer's WRITE operation to the reader's READ operation.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, dag_idx_1 = "fake_actor_1", 1
        fake_actor_2, dag_idx_2 = "fake_actor_2", 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[1].downstream_node_idxs = {2: fake_actor_2}

        actor_to_operation_nodes = {
            fake_actor_1: [
                list(
                    generate_dag_graph_nodes(0, dag_idx_1, fake_actor_1, False).values()
                )
            ],
            fake_actor_2: [
                list(
                    generate_dag_graph_nodes(0, dag_idx_2, fake_actor_2, False).values()
                )
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, dag_idx_1, [(0, 1), (1, 1), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, dag_idx_2, [(1, 1), (1, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, dag_idx_1, dag_idx_2)

    def test_edge_between_compute_nodes(self, monkeypatch):
        """
        driver -> fake_actor.op -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the COMPUTE operation with `bind_index` i to the COMPUTE operation with
        `bind_index` i+1 if they belong to the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor = "fake_actor"
        dag_idx_1, dag_idx_2 = 1, 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            dag_idx_1: CompiledTask(dag_idx_1, ClassMethodNode()),
            dag_idx_2: CompiledTask(dag_idx_2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[dag_idx_1].downstream_node_idxs = {dag_idx_2: fake_actor}

        actor_to_operation_nodes = {
            fake_actor: [
                list(
                    generate_dag_graph_nodes(0, dag_idx_1, fake_actor, False).values()
                ),
                list(
                    generate_dag_graph_nodes(1, dag_idx_2, fake_actor, False).values()
                ),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, dag_idx_1, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, dag_idx_2, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, dag_idx_1, dag_idx_2)
        self.check_edge_between_compute_nodes(graph, dag_idx_1, dag_idx_2)

    def test_two_actors(self, monkeypatch):
        """
        driver -> fake_actor_1.op -> fake_actor_2.op -> driver
               |                                     |
               -> fake_actor_2.op -> fake_actor_1.op -

        This test includes two actors, each with two tasks. The
        test case covers all three rules for adding edges between
        operation nodes in the operation graph.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, dag_idx_1, dag_idx_3 = "fake_actor_1", 1, 3
        fake_actor_2, dag_idx_2, dag_idx_4 = "fake_actor_2", 2, 4

        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            dag_idx_1: CompiledTask(dag_idx_1, ClassMethodNode()),
            dag_idx_2: CompiledTask(dag_idx_2, ClassMethodNode()),
            dag_idx_3: CompiledTask(dag_idx_3, ClassMethodNode()),
            dag_idx_4: CompiledTask(dag_idx_4, ClassMethodNode()),
            5: CompiledTask(5, MultiOutputNode()),
        }
        idx_to_task[dag_idx_1].downstream_node_idxs = {dag_idx_4: fake_actor_2}
        idx_to_task[dag_idx_2].downstream_node_idxs = {dag_idx_3: fake_actor_1}

        actor_to_operation_nodes = {
            fake_actor_1: [
                list(
                    generate_dag_graph_nodes(0, dag_idx_1, fake_actor_1, False).values()
                ),
                list(
                    generate_dag_graph_nodes(1, dag_idx_3, fake_actor_1, False).values()
                ),
            ],
            fake_actor_2: [
                list(
                    generate_dag_graph_nodes(0, dag_idx_2, fake_actor_2, False).values()
                ),
                list(
                    generate_dag_graph_nodes(1, dag_idx_4, fake_actor_2, False).values()
                ),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 4

        self.check_edges_between_read_compute_write(
            graph, dag_idx_1, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, dag_idx_2, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, dag_idx_3, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edges_between_read_compute_write(
            graph, dag_idx_4, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, dag_idx_1, dag_idx_4)
        self.check_edge_between_writer_and_reader(graph, dag_idx_2, dag_idx_3)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
