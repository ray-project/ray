# coding: utf-8
import os
import sys
from typing import Dict, List, Tuple

import pytest

from ray.actor import ActorHandle
from ray.dag import ClassMethodNode, InputNode, MultiOutputNode
from ray.dag.compiled_dag_node import CompiledTask
from ray.dag.dag_node_operation import (
    _add_edge,
    _build_dag_node_operation_graph,
    _DAGNodeOperation,
    _DAGNodeOperationType,
    _DAGOperationGraphNode,
    _extract_execution_schedule,
    _generate_actor_to_execution_schedule,
    _select_next_nodes,
)
from ray.tests.conftest import *  # noqa

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


def mock_actor_handle_init(self, actor_id: str):
    self._ray_actor_id = actor_id


def mock_class_method_call_init(self):
    self._is_class_method_output = False


def mock_init(self):
    pass


def generate_dag_graph_nodes(
    exec_task_idx,
    task_idx,
    actor_handle,
    requires_nccl_read=False,
    requires_nccl_compute=False,
    requires_nccl_write=False,
):
    graph_nodes = {}
    for op_type in _DAGNodeOperationType:
        requires_nccl = (
            (op_type == _DAGNodeOperationType.READ and requires_nccl_read)
            or (op_type == _DAGNodeOperationType.COMPUTE and requires_nccl_compute)
            or (op_type == _DAGNodeOperationType.WRITE and requires_nccl_write)
        )
        graph_nodes[op_type] = _DAGOperationGraphNode(
            _DAGNodeOperation(exec_task_idx, op_type),
            task_idx,
            actor_handle,
            requires_nccl,
        )
    return graph_nodes


def set_sync_idxs_p2p(
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
    write_idx: int,
    read_idx: int,
) -> None:
    write_node = graph[write_idx][_DAGNodeOperationType.WRITE]
    read_node = graph[read_idx][_DAGNodeOperationType.READ]
    p2p_idxs = {
        (write_idx, _DAGNodeOperationType.WRITE),
        (read_idx, _DAGNodeOperationType.READ),
    }
    for node in [write_node, read_node]:
        node.sync_idxs.update(p2p_idxs)
        node.pending_sync_idxs.update(p2p_idxs)


def set_sync_idxs_collective(
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
    task_idxs: List[int],
) -> None:
    collective_idxs = {
        (task_idx, _DAGNodeOperationType.COMPUTE) for task_idx in task_idxs
    }
    for task_idx in task_idxs:
        node = graph[task_idx][_DAGNodeOperationType.COMPUTE]
        node.sync_idxs.update(collective_idxs)
        node.pending_sync_idxs.update(collective_idxs)


def _generate_and_extract_execution_schedule(graph):
    return _extract_execution_schedule(_generate_actor_to_execution_schedule(graph))


class TestSelectNextNodes:
    """
    Test whether `_select_next_nodes` function selects the next nodes for
    topological sort to generate execution schedule correctly.

    task_idx: Each DAG node has a unique global index.
    exec_task_idx: The DAG node's index in the actor's `executable_tasks` list.
    """

    def test_two_candidates_on_same_actor(self, monkeypatch):
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
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)
        fake_actor = ActorHandle("fake_actor")
        # The DAG node has a global index of 1, and its index in the
        # actor's `executable_tasks` list is 0.
        task_idx_1 = 1
        dag_node_1 = _DAGOperationGraphNode(
            _DAGNodeOperation(0, _DAGNodeOperationType.READ),
            task_idx_1,
            fake_actor,
            False,
        )
        # The DAG node has a global index of 2, and its index in the
        # actor's `executable_tasks` list is 1.
        task_idx_2 = 2
        dag_node_2 = _DAGOperationGraphNode(
            _DAGNodeOperation(1, _DAGNodeOperationType.READ),
            task_idx_2,
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
        fake_actor_1, task_idx_1, exec_task_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, task_idx_2, exec_task_idx_2 = ActorHandle("fake_actor_2"), 2, 0
        mock_graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1,
                task_idx_1,
                fake_actor_1,
                requires_nccl_write=True,
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2,
                task_idx_2,
                fake_actor_2,
                requires_nccl_read=True,
            ),
        }
        del mock_graph[task_idx_1][_DAGNodeOperationType.READ]
        del mock_graph[task_idx_1][_DAGNodeOperationType.COMPUTE]

        _add_edge(
            mock_graph[task_idx_1][_DAGNodeOperationType.WRITE],
            mock_graph[task_idx_2][_DAGNodeOperationType.READ],
        )
        _add_edge(
            mock_graph[task_idx_2][_DAGNodeOperationType.READ],
            mock_graph[task_idx_2][_DAGNodeOperationType.COMPUTE],
        )
        _add_edge(
            mock_graph[task_idx_2][_DAGNodeOperationType.COMPUTE],
            mock_graph[task_idx_2][_DAGNodeOperationType.WRITE],
        )
        set_sync_idxs_p2p(mock_graph, task_idx_1, task_idx_2)
        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[task_idx_1][_DAGNodeOperationType.WRITE]],
            fake_actor_2: [mock_graph[task_idx_2][_DAGNodeOperationType.READ]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert next_nodes == [
            mock_graph[task_idx_1][_DAGNodeOperationType.WRITE],
            mock_graph[task_idx_2][_DAGNodeOperationType.READ],
        ]

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
        task_idx_1_0, exec_task_idx_1_0 = 1, 0
        task_idx_1_1, exec_task_idx_1_1 = 3, 1
        fake_actor_2 = ActorHandle("fake_actor_2")
        task_idx_2_0, exec_task_idx_2_0 = 2, 0
        task_idx_2_1, exec_task_idx_2_1 = 4, 1

        # Run the test 10 times to ensure that the result of `_select_next_nodes`
        # is deterministic.
        for _ in range(20):
            mock_graph = {
                task_idx_1_0: generate_dag_graph_nodes(
                    exec_task_idx_1_0,
                    task_idx_1_0,
                    fake_actor_1,
                    requires_nccl_write=True,
                ),
                task_idx_1_1: generate_dag_graph_nodes(
                    exec_task_idx_1_1,
                    task_idx_1_1,
                    fake_actor_1,
                    requires_nccl_read=True,
                ),
                task_idx_2_0: generate_dag_graph_nodes(
                    exec_task_idx_2_0,
                    task_idx_2_0,
                    fake_actor_2,
                    requires_nccl_write=True,
                ),
                task_idx_2_1: generate_dag_graph_nodes(
                    exec_task_idx_2_1,
                    task_idx_2_1,
                    fake_actor_2,
                    requires_nccl_read=True,
                ),
            }
            del mock_graph[task_idx_1_0][_DAGNodeOperationType.READ]
            del mock_graph[task_idx_1_0][_DAGNodeOperationType.COMPUTE]
            del mock_graph[task_idx_2_0][_DAGNodeOperationType.READ]
            del mock_graph[task_idx_2_0][_DAGNodeOperationType.COMPUTE]

            _add_edge(
                mock_graph[task_idx_1_0][_DAGNodeOperationType.WRITE],
                mock_graph[task_idx_2_1][_DAGNodeOperationType.READ],
            )
            _add_edge(
                mock_graph[task_idx_2_0][_DAGNodeOperationType.WRITE],
                mock_graph[task_idx_1_1][_DAGNodeOperationType.READ],
            )
            _add_edge(
                mock_graph[task_idx_2_1][_DAGNodeOperationType.READ],
                mock_graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE],
            )
            _add_edge(
                mock_graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE],
                mock_graph[task_idx_2_1][_DAGNodeOperationType.WRITE],
            )
            _add_edge(
                mock_graph[task_idx_1_1][_DAGNodeOperationType.READ],
                mock_graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE],
            )
            _add_edge(
                mock_graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE],
                mock_graph[task_idx_1_1][_DAGNodeOperationType.WRITE],
            )
            set_sync_idxs_p2p(mock_graph, task_idx_1_0, task_idx_2_1)
            set_sync_idxs_p2p(mock_graph, task_idx_2_0, task_idx_1_1)
            mock_actor_to_candidates = {
                fake_actor_1: [
                    mock_graph[task_idx_1_0][_DAGNodeOperationType.WRITE],
                    mock_graph[task_idx_1_1][_DAGNodeOperationType.READ],
                ],
                fake_actor_2: [
                    mock_graph[task_idx_2_0][_DAGNodeOperationType.WRITE],
                    mock_graph[task_idx_2_1][_DAGNodeOperationType.READ],
                ],
            }

            next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
            assert next_nodes == [
                mock_graph[task_idx_1_0][_DAGNodeOperationType.WRITE],
                mock_graph[task_idx_2_1][_DAGNodeOperationType.READ],
            ]

    def test_only_one_nccl_collective(self, monkeypatch):
        """
        Simulate the case where there is only one candidate which is a NCCL
        collective operation. In this case, `_select_next_nodes` should return
        all the NCCL collective nodes.

        driver -> fake_actor_1.allreduce_1 -> driver
               |                            |
               -> fake_actor_2.allreduce_1 ->
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)
        fake_actor_1, dag_idx_1, local_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, dag_idx_2, local_idx_2 = ActorHandle("fake_actor_2"), 2, 0

        mock_graph = {
            dag_idx_1: generate_dag_graph_nodes(
                local_idx_1,
                dag_idx_1,
                fake_actor_1,
                requires_nccl_compute=True,
            ),
            dag_idx_2: generate_dag_graph_nodes(
                local_idx_2,
                dag_idx_2,
                fake_actor_2,
                requires_nccl_compute=True,
            ),
        }
        set_sync_idxs_collective(mock_graph, [dag_idx_1, dag_idx_2])

        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[dag_idx_1][_DAGNodeOperationType.COMPUTE]],
            fake_actor_2: [mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[dag_idx_1][_DAGNodeOperationType.COMPUTE],
            mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE],
        }

    def test_two_nccl_collectives(self, monkeypatch):
        """
        Simulate the case where there are two candidates that are NCCL collective
        operations. In this case, `_select_next_nodes` should return all the NCCL
        collective nodes that are bond earlier.

        driver -> fake_actor_1.allreduce_1 -> driver
               |                            |
               -> fake_actor_2.allreduce_1 ->
               |                            |
               -> fake_actor_3.allreduce_2 ->
               |                            |
               -> fake_actor_4.allreduce_2 ->
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)
        fake_actor_1, dag_idx_1, local_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, dag_idx_2, local_idx_2 = ActorHandle("fake_actor_2"), 2, 0
        fake_actor_3, dag_idx_3, local_idx_3 = ActorHandle("fake_actor_3"), 3, 0
        fake_actor_4, dag_idx_4, local_idx_4 = ActorHandle("fake_actor_4"), 4, 0

        mock_graph = {
            dag_idx_1: generate_dag_graph_nodes(
                local_idx_1,
                dag_idx_1,
                fake_actor_1,
                requires_nccl_compute=True,
            ),
            dag_idx_2: generate_dag_graph_nodes(
                local_idx_2,
                dag_idx_2,
                fake_actor_2,
                requires_nccl_compute=True,
            ),
            dag_idx_3: generate_dag_graph_nodes(
                local_idx_3,
                dag_idx_3,
                fake_actor_3,
                requires_nccl_compute=True,
            ),
            dag_idx_4: generate_dag_graph_nodes(
                local_idx_4,
                dag_idx_4,
                fake_actor_4,
                requires_nccl_compute=True,
            ),
        }
        set_sync_idxs_collective(mock_graph, [dag_idx_1, dag_idx_2])
        set_sync_idxs_collective(mock_graph, [dag_idx_3, dag_idx_4])

        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[dag_idx_1][_DAGNodeOperationType.COMPUTE]],
            fake_actor_2: [mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE]],
            fake_actor_3: [mock_graph[dag_idx_3][_DAGNodeOperationType.COMPUTE]],
            fake_actor_4: [mock_graph[dag_idx_4][_DAGNodeOperationType.COMPUTE]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[dag_idx_1][_DAGNodeOperationType.COMPUTE],
            mock_graph[dag_idx_2][_DAGNodeOperationType.COMPUTE],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[dag_idx_3][_DAGNodeOperationType.COMPUTE],
            mock_graph[dag_idx_4][_DAGNodeOperationType.COMPUTE],
        }


class TestBuildDAGNodeOperationGraph:
    """
    Test whether `_build_dag_node_operation_graph` function adds the correct
    edges between the nodes in the operation graph base on the 3 rules mentioned
    in the doc string of `_build_dag_node_operation_graph`.
    """

    def check_edges_between_read_compute_write(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        task_idx: int,
        expected_num_edges: List[Tuple[int, int]],
    ):
        """
        Check whether edges from READ to COMPUTE, and from COMPUTE to WRITE,
        belonging to the same task are added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            task_idx: The global index of the task used to access the task in
                `idx_to_task`.
            expected_num_edges: A list of tuples where each tuple contains the expected
                number of in-edges and out-edges for READ, COMPUTE, and WRITE
                operations.
        """
        assert len(expected_num_edges) == 3
        assert len(graph[task_idx]) == 3
        read_node = graph[task_idx][_DAGNodeOperationType.READ]
        compute_node = graph[task_idx][_DAGNodeOperationType.COMPUTE]
        write_node = graph[task_idx][_DAGNodeOperationType.WRITE]

        for idx, node in enumerate([read_node, compute_node, write_node]):
            assert node.in_degree == expected_num_edges[idx][0]
            assert len(node.out_edges) == expected_num_edges[idx][1]

        assert (task_idx, _DAGNodeOperationType.COMPUTE) in read_node.out_edges
        assert (task_idx, _DAGNodeOperationType.READ) in compute_node.in_edges
        assert (task_idx, _DAGNodeOperationType.WRITE) in compute_node.out_edges
        assert (task_idx, _DAGNodeOperationType.COMPUTE) in write_node.in_edges

    def check_edge_between_writer_and_reader(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        writer_task_idx: int,
        reader_task_idx: int,
    ):
        """
        Check whether the edge from writer's WRITE to reader's READ operation is added.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            writer_task_idx: The index of the task used to access the task
                that the writer belongs to in `idx_to_task`.
            reader_task_idx: The index of the task used to access the task
                that the reader belongs to in `idx_to_task`.
        """
        write_node = graph[writer_task_idx][_DAGNodeOperationType.WRITE]
        read_node = graph[reader_task_idx][_DAGNodeOperationType.READ]

        assert (reader_task_idx, _DAGNodeOperationType.READ) in write_node.out_edges
        assert (writer_task_idx, _DAGNodeOperationType.WRITE) in read_node.in_edges

    def check_edge_between_compute_nodes(
        self,
        graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
        task_idx_1: int,
        task_idx_2: int,
    ):
        """
        Check whether the edge from COMPUTE with `bind_index` i to COMPUTE with
            `bind_index` i+1 if they belong to the same actor.

        Args:
            graph: The operation graph generated by `_build_dag_node_operation_graph`.
            task_idx_1: The index of the task used to access the task in
                `idx_to_task`.
            task_idx_2: The index of the task used to access the task in
                `idx_to_task`. Note that both tasks belong to the same actor, and the
                `bind_index` of the second task is equal to the `bind_index` of the
                first task plus one.
        """
        compute_node_1 = graph[task_idx_1][_DAGNodeOperationType.COMPUTE]
        compute_node_2 = graph[task_idx_2][_DAGNodeOperationType.COMPUTE]

        assert (task_idx_2, _DAGNodeOperationType.COMPUTE) in compute_node_1.out_edges
        assert (task_idx_1, _DAGNodeOperationType.COMPUTE) in compute_node_2.in_edges

    def test_edges_between_read_compute_write(self, monkeypatch):
        """
        driver -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds edges
        between READ/COMPUTE and COMPUTE/WRITE operations on the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, MultiOutputNode()),
        }

        fake_actor = "fake_actor"
        task_idx = 1
        actor_to_operation_nodes = {
            fake_actor: [
                list(generate_dag_graph_nodes(0, task_idx, fake_actor).values())
            ]
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 1

        self.check_edges_between_read_compute_write(
            graph, task_idx, [(0, 1), (1, 1), (1, 0)]
        )

    def test_edge_between_writer_and_reader(self, monkeypatch):
        """
        driver -> fake_actor_1.op -> fake_actor_2.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the writer's WRITE operation to the reader's READ operation.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, task_idx_1 = "fake_actor_1", 1
        fake_actor_2, task_idx_2 = "fake_actor_2", 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[1].downstream_task_idxs = {2: fake_actor_2}

        actor_to_operation_nodes = {
            fake_actor_1: [
                list(generate_dag_graph_nodes(0, task_idx_1, fake_actor_1).values())
            ],
            fake_actor_2: [
                list(generate_dag_graph_nodes(0, task_idx_2, fake_actor_2).values())
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, task_idx_1, [(0, 1), (1, 1), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, task_idx_2, [(1, 1), (1, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, task_idx_1, task_idx_2)

    def test_edge_between_compute_nodes(self, monkeypatch):
        """
        driver -> fake_actor.op -> fake_actor.op -> driver

        This test case aims to verify whether the function correctly adds an edge
        from the COMPUTE operation with `bind_index` i to the COMPUTE operation with
        `bind_index` i+1 if they belong to the same actor.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor = "fake_actor"
        task_idx_1, task_idx_2 = 1, 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            task_idx_1: CompiledTask(task_idx_1, ClassMethodNode()),
            task_idx_2: CompiledTask(task_idx_2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[task_idx_1].downstream_task_idxs = {task_idx_2: fake_actor}

        actor_to_operation_nodes = {
            fake_actor: [
                list(generate_dag_graph_nodes(0, task_idx_1, fake_actor).values()),
                list(generate_dag_graph_nodes(1, task_idx_2, fake_actor).values()),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edges_between_read_compute_write(
            graph, task_idx_1, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, task_idx_2, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, task_idx_1, task_idx_2)
        self.check_edge_between_compute_nodes(graph, task_idx_1, task_idx_2)

    def test_two_actors(self, monkeypatch):
        """
        driver -> fake_actor_1.op -> fake_actor_2.op -> driver
               |                                     |
               -> fake_actor_2.op -> fake_actor_1.op -

        This test includes two actors, each with two tasks. The
        test case covers all three rules for adding edges between
        operation nodes in the operation graph.
        """
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, task_idx_1, task_idx_3 = "fake_actor_1", 1, 3
        fake_actor_2, task_idx_2, task_idx_4 = "fake_actor_2", 2, 4

        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            task_idx_1: CompiledTask(task_idx_1, ClassMethodNode()),
            task_idx_2: CompiledTask(task_idx_2, ClassMethodNode()),
            task_idx_3: CompiledTask(task_idx_3, ClassMethodNode()),
            task_idx_4: CompiledTask(task_idx_4, ClassMethodNode()),
            5: CompiledTask(5, MultiOutputNode()),
        }
        idx_to_task[task_idx_1].downstream_task_idxs = {task_idx_4: fake_actor_2}
        idx_to_task[task_idx_2].downstream_task_idxs = {task_idx_3: fake_actor_1}

        actor_to_operation_nodes = {
            fake_actor_1: [
                list(generate_dag_graph_nodes(0, task_idx_1, fake_actor_1).values()),
                list(generate_dag_graph_nodes(1, task_idx_3, fake_actor_1).values()),
            ],
            fake_actor_2: [
                list(generate_dag_graph_nodes(0, task_idx_2, fake_actor_2).values()),
                list(generate_dag_graph_nodes(1, task_idx_4, fake_actor_2).values()),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 4

        self.check_edges_between_read_compute_write(
            graph, task_idx_1, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, task_idx_2, [(0, 1), (1, 2), (1, 1)]
        )
        self.check_edges_between_read_compute_write(
            graph, task_idx_3, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edges_between_read_compute_write(
            graph, task_idx_4, [(1, 1), (2, 1), (1, 0)]
        )
        self.check_edge_between_writer_and_reader(graph, task_idx_1, task_idx_4)
        self.check_edge_between_writer_and_reader(graph, task_idx_2, task_idx_3)


class TestGenerateActorToExecutionSchedule:
    """
    Test whether `_generate_actor_to_execution_schedule` function generates the
    correct execution schedule for each actor.
    """

    def add_edge_between_read_compute_write(
        self, operations: Dict[_DAGNodeOperationType, _DAGOperationGraphNode]
    ):
        """
        Add edges between READ and COMPUTE, and between COMPUTE and WRITE operations
        on the same actor.

        Args:
            operations: A dictionary where the key is the operation type and the value
                is the operation node.
        """
        assert len(operations) == 3
        _add_edge(
            operations[_DAGNodeOperationType.READ],
            operations[_DAGNodeOperationType.COMPUTE],
        )
        _add_edge(
            operations[_DAGNodeOperationType.COMPUTE],
            operations[_DAGNodeOperationType.WRITE],
        )

    def add_data_dependeny(
        self,
        writer_operations: Dict[_DAGNodeOperationType, _DAGOperationGraphNode],
        reader_operations: Dict[_DAGNodeOperationType, _DAGOperationGraphNode],
    ):
        """
        Add a data dependency between the WRITE operation of the writer and the READ
        operation of the reader.

        Args:
            writer_operations: A dictionary where the key is the operation type and the
                value is the operation node of the writer.
            reader_operations: A dictionary where the key is the operation type and the
                value is the operation node of the reader.
        """
        _add_edge(
            writer_operations[_DAGNodeOperationType.WRITE],
            reader_operations[_DAGNodeOperationType.READ],
        )

    def add_control_dependency(
        self,
        operations_1: Dict[_DAGNodeOperationType, _DAGOperationGraphNode],
        operations_2: Dict[_DAGNodeOperationType, _DAGOperationGraphNode],
    ):
        """
        Add a control dependency between the COMPUTE operation of the task with
        bind_index i and the COMPUTE operation of the task with bind_index i+1
        on the same actor.

        Args:
            operations_1: A dictionary where the key is the operation type and the value
                is the operation node of the task with bind_index i.
            operations_2: A dictionary where the key is the operation type and the value
                is the operation node of the task with bind_index i+1.
        """
        _add_edge(
            operations_1[_DAGNodeOperationType.COMPUTE],
            operations_2[_DAGNodeOperationType.COMPUTE],
        )

    def test_single_actor_1(self, monkeypatch):
        """
        driver -> fake_actor.op (task_idx_1) -> fake_actor.op (task_idx_2) -> driver

        Test the case where there is only one actor and no NCCL operations.
        Because there is no NCCL operation, all operations with smaller
        `bind_index` should be executed before the operations with larger
        `bind_index` on the same actor.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor = ActorHandle("fake_actor")
        task_idx_1, exec_task_idx_1 = 1, 0
        task_idx_2, exec_task_idx_2 = 2, 1
        graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1, task_idx_1, fake_actor
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2, task_idx_2, fake_actor
            ),
        }
        self.add_edge_between_read_compute_write(graph[task_idx_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2])
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_2])
        self.add_control_dependency(graph[task_idx_1], graph[task_idx_2])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 6
        assert actor_to_execution_schedule[fake_actor] == [
            graph[task_idx_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_single_actor_2(self, monkeypatch):
        """
        driver -> fake_actor.op (task_idx_1) -> fake_actor.op (task_idx_2) -> driver
                                            |                            |
                                            -> fake_actor.op (task_idx_3) -

        When the `dad_idx_1.WRITE` operation is picked, both `task_idx_2.READ` and
        `task_idx_3.READ` operations should be zero in-degree. In this case, the one
        with the smaller `bind_index` should be selected first. That is,
        `task_idx_2.READ` should be selected first.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor = ActorHandle("fake_actor")
        task_idx_1, exec_task_idx_1 = 1, 0
        task_idx_2, exec_task_idx_2 = 2, 1
        task_idx_3, exec_task_idx_3 = 3, 2

        graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1, task_idx_1, fake_actor
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2, task_idx_2, fake_actor
            ),
            task_idx_3: generate_dag_graph_nodes(
                exec_task_idx_3, task_idx_3, fake_actor
            ),
        }
        self.add_edge_between_read_compute_write(graph[task_idx_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2])
        self.add_edge_between_read_compute_write(graph[task_idx_3])
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_2])
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_3])
        self.add_control_dependency(graph[task_idx_1], graph[task_idx_2])
        self.add_control_dependency(graph[task_idx_2], graph[task_idx_3])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 9
        assert actor_to_execution_schedule[fake_actor] == [
            graph[task_idx_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_3][_DAGNodeOperationType.READ].operation,
            graph[task_idx_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_3][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_two_actors_no_nccl(self, monkeypatch):
        """
        driver -> actor_1.op (task_idx_1_1) -> actor_2.op (task_idx_2_2) -> driver
               |                                                       |
               -> actor_2.op (task_idx_2_1) -> actor_1.op (task_idx_1_2) -

        Test the case where there are two actors and no NCCL operations.
        Because there is no NCCL operation, all operations with smaller
        `bind_index` should be executed before the operations with larger
        `bind_index` on the same actor.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        task_idx_1_1, exec_task_idx_1_1 = 1, 0
        task_idx_1_2, exec_task_idx_1_2 = 4, 1

        fake_actor_2 = ActorHandle("fake_actor_2")
        task_idx_2_1, exec_task_idx_2_1 = 2, 0
        task_idx_2_2, exec_task_idx_2_2 = 3, 1

        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1, task_idx_1_1, fake_actor_1
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1, task_idx_2_1, fake_actor_2
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2, task_idx_2_2, fake_actor_2
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2, task_idx_1_2, fake_actor_1
            ),
        }
        self.add_edge_between_read_compute_write(graph[task_idx_1_1])
        self.add_edge_between_read_compute_write(graph[task_idx_1_2])
        self.add_edge_between_read_compute_write(graph[task_idx_2_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_2_1], graph[task_idx_2_2])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 6
        assert len(actor_to_execution_schedule[fake_actor_2]) == 6

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[task_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            graph[task_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_two_actors_with_nccl(self, monkeypatch):
        """
        driver -> actor_1.op (task_idx_1_1) -> actor_2.op (task_idx_2_2) -> driver
               |                                                       |
               -> actor_2.op (task_idx_2_1) -> actor_1.op (task_idx_1_2) -

        In this test, the communication between fake_actor_1 and fake_actor_2 is done
        using NCCL. When the task_idx_1.WRITE operation is picked, the task_idx_2.READ
        operation is also added to the execution schedule because of the NCCL operation.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        task_idx_1_1, exec_task_idx_1_1 = 1, 0
        task_idx_1_2, exec_task_idx_1_2 = 4, 1

        fake_actor_2 = ActorHandle("fake_actor_2")
        task_idx_2_1, exec_task_idx_2_1 = 2, 0
        task_idx_2_2, exec_task_idx_2_2 = 3, 1

        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1,
                task_idx_1_1,
                fake_actor_1,
                requires_nccl_write=True,
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1,
                task_idx_2_1,
                fake_actor_2,
                requires_nccl_write=True,
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2,
                task_idx_2_2,
                fake_actor_2,
                requires_nccl_read=True,
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2,
                task_idx_1_2,
                fake_actor_1,
                requires_nccl_read=True,
            ),
        }
        set_sync_idxs_p2p(graph, task_idx_1_1, task_idx_2_2)
        set_sync_idxs_p2p(graph, task_idx_2_1, task_idx_1_2)
        self.add_edge_between_read_compute_write(graph[task_idx_1_1])
        self.add_edge_between_read_compute_write(graph[task_idx_1_2])
        self.add_edge_between_read_compute_write(graph[task_idx_2_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_2_1], graph[task_idx_2_2])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 6
        assert len(actor_to_execution_schedule[fake_actor_2]) == 6

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[task_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            # `actor_2.task_idx_2_2.READ` (P2P recv) is scheduled together with
            # `actor_1.task_idx_1_1.WRITE` (P2P send).
            graph[task_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_simulate_pp_2workers_2batches_1f1b_with_nccl(self, monkeypatch):
        """
        This test simulates a simple 1F1B pipeline parallelism for training with
        2 workers and 2 batches.

        w1: fwd_b1  fwd_b2          bwd_b1          bwd_b2
        w2:         fwd_b1  bwd_b1  fwd_b2  bwd_b2

        The communication between workers is done using NCCL. The communication
        within the worker actor is done using IntraProcessChannel.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        worker_1 = ActorHandle("worker_1")
        task_idx_1_1, exec_task_idx_1_1 = 1, 0
        task_idx_1_2, exec_task_idx_1_2 = 2, 1
        task_idx_1_3, exec_task_idx_1_3 = 3, 2
        task_idx_1_4, exec_task_idx_1_4 = 4, 3
        worker_2 = ActorHandle("worker_2")
        task_idx_2_1, exec_task_idx_2_1 = 5, 0
        task_idx_2_2, exec_task_idx_2_2 = 6, 1
        task_idx_2_3, exec_task_idx_2_3 = 7, 2
        task_idx_2_4, exec_task_idx_2_4 = 8, 3
        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1,
                task_idx_1_1,
                worker_1,
                requires_nccl_write=True,
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2,
                task_idx_1_2,
                worker_1,
                requires_nccl_write=True,
            ),
            task_idx_1_3: generate_dag_graph_nodes(
                exec_task_idx_1_3,
                task_idx_1_3,
                worker_1,
                requires_nccl_read=True,
            ),
            task_idx_1_4: generate_dag_graph_nodes(
                exec_task_idx_1_4,
                task_idx_1_4,
                worker_1,
                requires_nccl_read=True,
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1,
                task_idx_2_1,
                worker_2,
                requires_nccl_read=True,
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2,
                task_idx_2_2,
                worker_2,
                requires_nccl_write=True,
            ),
            task_idx_2_3: generate_dag_graph_nodes(
                exec_task_idx_2_3,
                task_idx_2_3,
                worker_2,
                requires_nccl_read=True,
            ),
            task_idx_2_4: generate_dag_graph_nodes(
                exec_task_idx_2_4,
                task_idx_2_4,
                worker_2,
                requires_nccl_write=True,
            ),
        }
        set_sync_idxs_p2p(graph, task_idx_1_1, task_idx_2_1)
        set_sync_idxs_p2p(graph, task_idx_1_2, task_idx_2_3)
        set_sync_idxs_p2p(graph, task_idx_2_2, task_idx_1_3)
        set_sync_idxs_p2p(graph, task_idx_2_4, task_idx_1_4)
        self.add_edge_between_read_compute_write(graph[task_idx_1_1])
        self.add_edge_between_read_compute_write(graph[task_idx_1_2])
        self.add_edge_between_read_compute_write(graph[task_idx_1_3])
        self.add_edge_between_read_compute_write(graph[task_idx_1_4])
        self.add_edge_between_read_compute_write(graph[task_idx_2_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2_2])
        self.add_edge_between_read_compute_write(graph[task_idx_2_3])
        self.add_edge_between_read_compute_write(graph[task_idx_2_4])
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_2_1])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_2], graph[task_idx_1_3])
        self.add_data_dependeny(graph[task_idx_1_2], graph[task_idx_2_3])
        self.add_data_dependeny(graph[task_idx_2_3], graph[task_idx_2_4])
        self.add_data_dependeny(graph[task_idx_2_4], graph[task_idx_1_4])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_1_2], graph[task_idx_1_3])
        self.add_control_dependency(graph[task_idx_1_3], graph[task_idx_1_4])
        self.add_control_dependency(graph[task_idx_2_1], graph[task_idx_2_2])
        self.add_control_dependency(graph[task_idx_2_2], graph[task_idx_2_3])
        self.add_control_dependency(graph[task_idx_2_3], graph[task_idx_2_4])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[worker_1]) == 12
        assert len(actor_to_execution_schedule[worker_2]) == 12
        assert actor_to_execution_schedule[worker_1] == [
            graph[task_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[task_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            # `actor_2.task_idx_2_3.READ` (P2P recv) is scheduled together with
            # `actor_1.task_idx_1_2.WRITE` (P2P send).
            graph[task_idx_2_3][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_3][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_simulate_pp_2workers_2batches_1f1b_no_nccl(self, monkeypatch):
        """
        This test simulates a simple 1F1B pipeline parallelism for training with
        2 workers and 2 batches.

        w1: fwd_b1  fwd_b2          bwd_b1          bwd_b2
        w2:         fwd_b1  bwd_b1  fwd_b2  bwd_b2

        Because there is no NCCL operation, all operations with smaller
        `bind_index` should be executed before the operations with larger
        `bind_index` on the same actor.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        worker_1 = ActorHandle("worker_1")
        task_idx_1_1, exec_task_idx_1_1 = 1, 0
        task_idx_1_2, exec_task_idx_1_2 = 2, 1
        task_idx_1_3, exec_task_idx_1_3 = 3, 2
        task_idx_1_4, exec_task_idx_1_4 = 4, 3
        worker_2 = ActorHandle("worker_2")
        task_idx_2_1, exec_task_idx_2_1 = 5, 0
        task_idx_2_2, exec_task_idx_2_2 = 6, 1
        task_idx_2_3, exec_task_idx_2_3 = 7, 2
        task_idx_2_4, exec_task_idx_2_4 = 8, 3

        # No NCCL operation.
        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1, task_idx_1_1, worker_1
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2, task_idx_1_2, worker_1
            ),
            task_idx_1_3: generate_dag_graph_nodes(
                exec_task_idx_1_3, task_idx_1_3, worker_1
            ),
            task_idx_1_4: generate_dag_graph_nodes(
                exec_task_idx_1_4, task_idx_1_4, worker_1
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1, task_idx_2_1, worker_2
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2, task_idx_2_2, worker_2
            ),
            task_idx_2_3: generate_dag_graph_nodes(
                exec_task_idx_2_3, task_idx_2_3, worker_2
            ),
            task_idx_2_4: generate_dag_graph_nodes(
                exec_task_idx_2_4, task_idx_2_4, worker_2
            ),
        }
        self.add_edge_between_read_compute_write(graph[task_idx_1_1])
        self.add_edge_between_read_compute_write(graph[task_idx_1_2])
        self.add_edge_between_read_compute_write(graph[task_idx_1_3])
        self.add_edge_between_read_compute_write(graph[task_idx_1_4])
        self.add_edge_between_read_compute_write(graph[task_idx_2_1])
        self.add_edge_between_read_compute_write(graph[task_idx_2_2])
        self.add_edge_between_read_compute_write(graph[task_idx_2_3])
        self.add_edge_between_read_compute_write(graph[task_idx_2_4])
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_2_1])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_2], graph[task_idx_1_3])
        self.add_data_dependeny(graph[task_idx_1_2], graph[task_idx_2_3])
        self.add_data_dependeny(graph[task_idx_2_3], graph[task_idx_2_4])
        self.add_data_dependeny(graph[task_idx_2_4], graph[task_idx_1_4])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_1_2], graph[task_idx_1_3])
        self.add_control_dependency(graph[task_idx_1_3], graph[task_idx_1_4])
        self.add_control_dependency(graph[task_idx_2_1], graph[task_idx_2_2])
        self.add_control_dependency(graph[task_idx_2_2], graph[task_idx_2_3])
        self.add_control_dependency(graph[task_idx_2_3], graph[task_idx_2_4])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[worker_1]) == 12
        assert len(actor_to_execution_schedule[worker_2]) == 12
        assert actor_to_execution_schedule[worker_1] == [
            graph[task_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_2][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_3][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.READ].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_1_4][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[task_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            # The order of `task_idx_2_3.READ` and `task_idx_2_2.WRITE` is important.
            # It is different from the case where there is an NCCL operation.
            graph[task_idx_2_2][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_3][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_3][_DAGNodeOperationType.WRITE].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.READ].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[task_idx_2_4][_DAGNodeOperationType.WRITE].operation,
        ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
