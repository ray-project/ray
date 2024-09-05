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
    _generate_actor_to_execution_schedule,
)
from ray.dag.compiled_dag_node import CompiledTask
from typing import List, Dict, Tuple
from ray.actor import ActorHandle

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


def mock_actor_handle_init(self, actor_id: str):
    self._ray_actor_id = actor_id


def mock_class_method_call_init(self):
    self._is_class_method_output = False


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
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
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
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor_1, dag_idx_1 = "fake_actor_1", 1
        fake_actor_2, dag_idx_2 = "fake_actor_2", 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            1: CompiledTask(1, ClassMethodNode()),
            2: CompiledTask(2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[1].downstream_task_idxs = {2: fake_actor_2}

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
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
        monkeypatch.setattr(MultiOutputNode, "__init__", mock_init)

        fake_actor = "fake_actor"
        dag_idx_1, dag_idx_2 = 1, 2
        idx_to_task = {
            0: CompiledTask(0, InputNode()),
            dag_idx_1: CompiledTask(dag_idx_1, ClassMethodNode()),
            dag_idx_2: CompiledTask(dag_idx_2, ClassMethodNode()),
            3: CompiledTask(3, MultiOutputNode()),
        }
        idx_to_task[dag_idx_1].downstream_task_idxs = {dag_idx_2: fake_actor}

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
        monkeypatch.setattr(ClassMethodNode, "__init__", mock_class_method_call_init)
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
        idx_to_task[dag_idx_1].downstream_task_idxs = {dag_idx_4: fake_actor_2}
        idx_to_task[dag_idx_2].downstream_task_idxs = {dag_idx_3: fake_actor_1}

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
        driver -> fake_actor.op (dag_idx_1) -> fake_actor.op (dag_idx_2) -> driver

        Test the case where there is only one actor and no NCCL operations.
        Because there is no NCCL operation, all operations with smaller
        `bind_index` should be executed before the operations with larger
        `bind_index` on the same actor.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor = ActorHandle("fake_actor")
        dag_idx_1, local_idx_1 = 1, 0
        dag_idx_2, local_idx_2 = 2, 1
        graph = {
            dag_idx_1: generate_dag_graph_nodes(
                local_idx_1, dag_idx_1, fake_actor, False
            ),
            dag_idx_2: generate_dag_graph_nodes(
                local_idx_2, dag_idx_2, fake_actor, False
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2])
        self.add_data_dependeny(graph[dag_idx_1], graph[dag_idx_2])
        self.add_control_dependency(graph[dag_idx_1], graph[dag_idx_2])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 6
        assert actor_to_execution_schedule[fake_actor] == [
            graph[dag_idx_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_single_actor_2(self, monkeypatch):
        """
        driver -> fake_actor.op (dag_idx_1) -> fake_actor.op (dag_idx_2) -> driver
                                            |                            |
                                            -> fake_actor.op (dag_idx_3) -

        When the `dad_idx_1.WRITE` operation is picked, both `dag_idx_2.READ` and
        `dag_idx_3.READ` operations should be zero in-degree. In this case, the one
        with the smaller `bind_index` should be selected first. That is,
        `dag_idx_2.READ` should be selected first.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor = ActorHandle("fake_actor")
        dag_idx_1, local_idx_1 = 1, 0
        dag_idx_2, local_idx_2 = 2, 1
        dag_idx_3, local_idx_3 = 3, 2

        graph = {
            dag_idx_1: generate_dag_graph_nodes(
                local_idx_1, dag_idx_1, fake_actor, False
            ),
            dag_idx_2: generate_dag_graph_nodes(
                local_idx_2, dag_idx_2, fake_actor, False
            ),
            dag_idx_3: generate_dag_graph_nodes(
                local_idx_3, dag_idx_3, fake_actor, False
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_3])
        self.add_data_dependeny(graph[dag_idx_1], graph[dag_idx_2])
        self.add_data_dependeny(graph[dag_idx_1], graph[dag_idx_3])
        self.add_control_dependency(graph[dag_idx_1], graph[dag_idx_2])
        self.add_control_dependency(graph[dag_idx_2], graph[dag_idx_3])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 9
        assert actor_to_execution_schedule[fake_actor] == [
            graph[dag_idx_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_3][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_3][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_two_actors_no_nccl(self, monkeypatch):
        """
        driver -> actor_1.op (dag_idx_1_1) -> actor_2.op (dag_idx_2_2) -> driver
               |                                                       |
               -> actor_2.op (dag_idx_2_1) -> actor_1.op (dag_idx_1_2) -

        Test the case where there are two actors and no NCCL operations.
        Because there is no NCCL operation, all operations with smaller
        `bind_index` should be executed before the operations with larger
        `bind_index` on the same actor.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        dag_idx_1_1, local_idx_1_1 = 1, 0
        dag_idx_1_2, local_idx_1_2 = 4, 1

        fake_actor_2 = ActorHandle("fake_actor_2")
        dag_idx_2_1, local_idx_2_1 = 2, 0
        dag_idx_2_2, local_idx_2_2 = 3, 1

        graph = {
            dag_idx_1_1: generate_dag_graph_nodes(
                local_idx_1_1, dag_idx_1_1, fake_actor_1, False
            ),
            dag_idx_2_1: generate_dag_graph_nodes(
                local_idx_2_1, dag_idx_2_1, fake_actor_2, False
            ),
            dag_idx_2_2: generate_dag_graph_nodes(
                local_idx_2_2, dag_idx_2_2, fake_actor_2, False
            ),
            dag_idx_1_2: generate_dag_graph_nodes(
                local_idx_1_2, dag_idx_1_2, fake_actor_1, False
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_1_1], graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_2_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_1_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_2_1], graph[dag_idx_2_2])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 6
        assert len(actor_to_execution_schedule[fake_actor_2]) == 6

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[dag_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            graph[dag_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.WRITE].operation,
        ]

    def test_two_actors_with_nccl(self, monkeypatch):
        """
        driver -> actor_1.op (dag_idx_1_1) -> actor_2.op (dag_idx_2_2) -> driver
               |                                                       |
               -> actor_2.op (dag_idx_2_1) -> actor_1.op (dag_idx_1_2) -

        In this test, the communication between fake_actor_1 and fake_actor_2 is done
        using NCCL. When the dag_idx_1.WRITE operation is picked, the dag_idx_2.READ
        operation is also added to the execution schedule because of the NCCL operation.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        dag_idx_1_1, local_idx_1_1 = 1, 0
        dag_idx_1_2, local_idx_1_2 = 4, 1

        fake_actor_2 = ActorHandle("fake_actor_2")
        dag_idx_2_1, local_idx_2_1 = 2, 0
        dag_idx_2_2, local_idx_2_2 = 3, 1

        graph = {
            dag_idx_1_1: generate_dag_graph_nodes(
                local_idx_1_1, dag_idx_1_1, fake_actor_1, True
            ),
            dag_idx_2_1: generate_dag_graph_nodes(
                local_idx_2_1, dag_idx_2_1, fake_actor_2, True
            ),
            dag_idx_2_2: generate_dag_graph_nodes(
                local_idx_2_2, dag_idx_2_2, fake_actor_2, False
            ),
            dag_idx_1_2: generate_dag_graph_nodes(
                local_idx_1_2, dag_idx_1_2, fake_actor_1, False
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_1_1], graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_2_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_1_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_2_1], graph[dag_idx_2_2])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 6
        assert len(actor_to_execution_schedule[fake_actor_2]) == 6

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[dag_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            graph[dag_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            # The order of `dag_idx_2_2.READ` and `dag_idx_2_2.COMPUTE` is important.
            graph[dag_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.WRITE].operation,
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
        dag_idx_1_1, local_idx_1_1 = 1, 0
        dag_idx_1_2, local_idx_1_2 = 2, 1
        dag_idx_1_3, local_idx_1_3 = 3, 2
        dag_idx_1_4, local_idx_1_4 = 4, 3
        worker_2 = ActorHandle("worker_2")
        dag_idx_2_1, local_idx_2_1 = 5, 0
        dag_idx_2_2, local_idx_2_2 = 6, 1
        dag_idx_2_3, local_idx_2_3 = 7, 2
        dag_idx_2_4, local_idx_2_4 = 8, 3
        graph = {
            dag_idx_1_1: generate_dag_graph_nodes(
                local_idx_1_1, dag_idx_1_1, worker_1, True
            ),
            dag_idx_1_2: generate_dag_graph_nodes(
                local_idx_1_2, dag_idx_1_2, worker_1, True
            ),
            dag_idx_1_3: generate_dag_graph_nodes(
                local_idx_1_3, dag_idx_1_3, worker_1, False
            ),
            dag_idx_1_4: generate_dag_graph_nodes(
                local_idx_1_4, dag_idx_1_4, worker_1, False
            ),
            dag_idx_2_1: generate_dag_graph_nodes(
                local_idx_2_1, dag_idx_2_1, worker_2, False
            ),
            dag_idx_2_2: generate_dag_graph_nodes(
                local_idx_2_2, dag_idx_2_2, worker_2, True
            ),
            dag_idx_2_3: generate_dag_graph_nodes(
                local_idx_2_3, dag_idx_2_3, worker_2, False
            ),
            dag_idx_2_4: generate_dag_graph_nodes(
                local_idx_2_4, dag_idx_2_4, worker_2, True
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_3])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_4])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_3])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_4])
        self.add_data_dependeny(graph[dag_idx_1_1], graph[dag_idx_2_1])
        self.add_data_dependeny(graph[dag_idx_2_1], graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_2_2], graph[dag_idx_1_3])
        self.add_data_dependeny(graph[dag_idx_1_2], graph[dag_idx_2_3])
        self.add_data_dependeny(graph[dag_idx_2_3], graph[dag_idx_2_4])
        self.add_data_dependeny(graph[dag_idx_2_4], graph[dag_idx_1_4])
        self.add_control_dependency(graph[dag_idx_1_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_1_2], graph[dag_idx_1_3])
        self.add_control_dependency(graph[dag_idx_1_3], graph[dag_idx_1_4])
        self.add_control_dependency(graph[dag_idx_2_1], graph[dag_idx_2_2])
        self.add_control_dependency(graph[dag_idx_2_2], graph[dag_idx_2_3])
        self.add_control_dependency(graph[dag_idx_2_3], graph[dag_idx_2_4])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[worker_1]) == 12
        assert len(actor_to_execution_schedule[worker_2]) == 12
        assert actor_to_execution_schedule[worker_1] == [
            graph[dag_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[dag_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            # The order of `dag_idx_2_3.READ` and `dag_idx_2_2.WRITE` is important.
            graph[dag_idx_2_3][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_3][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.WRITE].operation,
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
        dag_idx_1_1, local_idx_1_1 = 1, 0
        dag_idx_1_2, local_idx_1_2 = 2, 1
        dag_idx_1_3, local_idx_1_3 = 3, 2
        dag_idx_1_4, local_idx_1_4 = 4, 3
        worker_2 = ActorHandle("worker_2")
        dag_idx_2_1, local_idx_2_1 = 5, 0
        dag_idx_2_2, local_idx_2_2 = 6, 1
        dag_idx_2_3, local_idx_2_3 = 7, 2
        dag_idx_2_4, local_idx_2_4 = 8, 3

        # No NCCL operation.
        graph = {
            dag_idx_1_1: generate_dag_graph_nodes(
                local_idx_1_1, dag_idx_1_1, worker_1, False
            ),
            dag_idx_1_2: generate_dag_graph_nodes(
                local_idx_1_2, dag_idx_1_2, worker_1, False
            ),
            dag_idx_1_3: generate_dag_graph_nodes(
                local_idx_1_3, dag_idx_1_3, worker_1, False
            ),
            dag_idx_1_4: generate_dag_graph_nodes(
                local_idx_1_4, dag_idx_1_4, worker_1, False
            ),
            dag_idx_2_1: generate_dag_graph_nodes(
                local_idx_2_1, dag_idx_2_1, worker_2, False
            ),
            dag_idx_2_2: generate_dag_graph_nodes(
                local_idx_2_2, dag_idx_2_2, worker_2, False
            ),
            dag_idx_2_3: generate_dag_graph_nodes(
                local_idx_2_3, dag_idx_2_3, worker_2, False
            ),
            dag_idx_2_4: generate_dag_graph_nodes(
                local_idx_2_4, dag_idx_2_4, worker_2, False
            ),
        }
        self.add_edge_between_read_compute_write(graph[dag_idx_1_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_3])
        self.add_edge_between_read_compute_write(graph[dag_idx_1_4])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_1])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_2])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_3])
        self.add_edge_between_read_compute_write(graph[dag_idx_2_4])
        self.add_data_dependeny(graph[dag_idx_1_1], graph[dag_idx_2_1])
        self.add_data_dependeny(graph[dag_idx_2_1], graph[dag_idx_2_2])
        self.add_data_dependeny(graph[dag_idx_2_2], graph[dag_idx_1_3])
        self.add_data_dependeny(graph[dag_idx_1_2], graph[dag_idx_2_3])
        self.add_data_dependeny(graph[dag_idx_2_3], graph[dag_idx_2_4])
        self.add_data_dependeny(graph[dag_idx_2_4], graph[dag_idx_1_4])
        self.add_control_dependency(graph[dag_idx_1_1], graph[dag_idx_1_2])
        self.add_control_dependency(graph[dag_idx_1_2], graph[dag_idx_1_3])
        self.add_control_dependency(graph[dag_idx_1_3], graph[dag_idx_1_4])
        self.add_control_dependency(graph[dag_idx_2_1], graph[dag_idx_2_2])
        self.add_control_dependency(graph[dag_idx_2_2], graph[dag_idx_2_3])
        self.add_control_dependency(graph[dag_idx_2_3], graph[dag_idx_2_4])

        actor_to_execution_schedule = _generate_actor_to_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[worker_1]) == 12
        assert len(actor_to_execution_schedule[worker_2]) == 12
        assert actor_to_execution_schedule[worker_1] == [
            graph[dag_idx_1_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_2][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_3][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_1_4][_DAGNodeOperationType.WRITE].operation,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[dag_idx_2_1][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_1][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_2][_DAGNodeOperationType.COMPUTE].operation,
            # The order of `dag_idx_2_3.READ` and `dag_idx_2_2.WRITE` is important.
            # It is different from the case where there is an NCCL operation.
            graph[dag_idx_2_2][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_3][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_3][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_3][_DAGNodeOperationType.WRITE].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.READ].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.COMPUTE].operation,
            graph[dag_idx_2_4][_DAGNodeOperationType.WRITE].operation,
        ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
