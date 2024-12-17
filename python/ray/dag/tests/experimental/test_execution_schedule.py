# coding: utf-8
import os
import sys

import pytest

from ray.tests.conftest import *  # noqa
from ray.dag import InputNode, MultiOutputNode, ClassMethodNode
from ray.dag.dag_node_operation import (
    _DAGOperationGraphNode,
    _DAGNodeOperation,
    _extract_execution_schedule,
    _select_next_nodes,
    _build_dag_node_operation_graph,
    _add_edge,
    _generate_actor_to_execution_schedule,
)
from ray.dag.compiled_dag_node import CompiledTask
from ray.dag.nccl_operation import _NcclOperation
from ray.experimental.util.types import P2POp, ReduceOp
from typing import Dict, List, Set
from ray.actor import ActorHandle

if sys.platform != "linux" and sys.platform != "darwin":
    pytest.skip("Skipping, requires Linux or Mac.", allow_module_level=True)


def mock_actor_handle_init(self, actor_id: str):
    self._ray_actor_id = actor_id


def mock_class_method_call_init(self):
    self._is_class_method_output = False


def mock_init(self):
    pass


class MockSyncGroup(_NcclOperation):
    def __init__(self, task_idxs: List[int], ready_task_idxs: Set[int] = None):
        super().__init__()
        self.task_idxs = task_idxs
        if ready_task_idxs is not None:
            self.ready_task_idxs = ready_task_idxs

    def execute(self, *args, **kwargs):
        raise NotImplementedError


def generate_dag_graph_nodes(
    exec_task_idx,
    task_idx,
    actor_handle,
    nccl_op=None,
    # requires_nccl_read=False,
    # requires_nccl_write=False,
    # requires_nccl_collective=False,
    nccl_op_type=None,
) -> _DAGOperationGraphNode:
    node = _DAGOperationGraphNode(
        _DAGNodeOperation(exec_task_idx),
        task_idx,
        actor_handle,
        nccl_op,
        # requires_nccl_read,
        # requires_nccl_write,
        # requires_nccl_collective,
        nccl_op_type,
    )
    return node


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
            _DAGNodeOperation(0),
            task_idx_1,
            fake_actor,
        )
        # The DAG node has a global index of 2, and its index in the
        # actor's `executable_tasks` list is 1.
        task_idx_2 = 2
        dag_node_2 = _DAGOperationGraphNode(
            _DAGNodeOperation(1),
            task_idx_2,
            fake_actor,
        )
        mock_actor_to_candidates = {
            fake_actor: [
                dag_node_1,
                dag_node_2,
            ],
        }
        # The graph is not accessed because there are no NCCL ops.
        next_nodes = _select_next_nodes(mock_actor_to_candidates, dict())
        assert len(next_nodes) == 1
        assert next_nodes[0] == dag_node_1

        next_nodes = _select_next_nodes(mock_actor_to_candidates, dict())
        assert len(next_nodes) == 1
        assert next_nodes[0] == dag_node_2

        next_nodes = _select_next_nodes(mock_actor_to_candidates, dict())
        assert next_nodes is None

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
        nccl_op = MockSyncGroup([task_idx_1, task_idx_2], {task_idx_1, task_idx_2})
        mock_graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1,
                task_idx_1,
                fake_actor_1,
                nccl_op,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2,
                task_idx_2,
                fake_actor_2,
                nccl_op,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
        }

        mock_actor_to_candidates = {
            fake_actor_2: [mock_graph[task_idx_2]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert len(next_nodes) == 2
        assert set(next_nodes) == {
            mock_graph[task_idx_1],
            mock_graph[task_idx_2],
        }

        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert next_nodes is None

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
        task_idx_2_0, exec_task_idx_2_0 = 2, 1
        task_idx_2_1, exec_task_idx_2_1 = 4, 2

        nccl_op_1 = MockSyncGroup(
            [task_idx_1_0, task_idx_2_1], {task_idx_1_0, task_idx_2_1}
        )
        nccl_op_2 = MockSyncGroup(
            [task_idx_2_0, task_idx_1_1], {task_idx_2_0, task_idx_1_1}
        )

        # [TODO:andyub] This is no longer deterministic.
        # When a.send, b.send are both candidates, we can either get a schedule like
        # [a.send & b.recv, b.send & a.recv] or [b.send & a.recv, a.send & b.recv].
        # Run the test 10 times to ensure that the result of `_select_next_nodes`
        # is deterministic.
        for _ in range(20):
            mock_graph = {
                task_idx_1_0: generate_dag_graph_nodes(
                    exec_task_idx_1_0,
                    task_idx_1_0,
                    fake_actor_1,
                    nccl_op_1,
                    # requires_nccl_write=True,
                    P2POp.SEND,
                ),
                task_idx_1_1: generate_dag_graph_nodes(
                    exec_task_idx_1_1,
                    task_idx_1_1,
                    fake_actor_1,
                    nccl_op_2,
                    # requires_nccl_read=True,
                    P2POp.RECV,
                ),
                task_idx_2_0: generate_dag_graph_nodes(
                    exec_task_idx_2_0,
                    task_idx_2_0,
                    fake_actor_2,
                    nccl_op_2,
                    # requires_nccl_write=True,
                    P2POp.SEND,
                ),
                task_idx_2_1: generate_dag_graph_nodes(
                    exec_task_idx_2_1,
                    task_idx_2_1,
                    fake_actor_2,
                    nccl_op_1,
                    # requires_nccl_read=True,
                    P2POp.RECV,
                ),
            }

            mock_actor_to_candidates = {
                fake_actor_1: [mock_graph[task_idx_1_0]],
                fake_actor_2: [mock_graph[task_idx_2_0]],
            }

            next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
            assert len(next_nodes) == 2
            assert set(next_nodes) == {
                mock_graph[task_idx_1_0],
                mock_graph[task_idx_2_1],
            }

            next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
            assert len(next_nodes) == 2
            assert set(next_nodes) == {
                mock_graph[task_idx_2_0],
                mock_graph[task_idx_1_1],
            }

            next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
            assert next_nodes is None

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

        fake_actor_1, task_idx_1, exec_task_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, task_idx_2, exec_task_idx_2 = ActorHandle("fake_actor_2"), 2, 0
        nccl_op = MockSyncGroup([task_idx_1, task_idx_2], {task_idx_1, task_idx_2})

        mock_graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1,
                task_idx_1,
                fake_actor_1,
                nccl_op,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2,
                task_idx_2,
                fake_actor_2,
                nccl_op,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
        }

        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[task_idx_1]],
            fake_actor_2: [mock_graph[task_idx_2]],
        }
        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[task_idx_1],
            mock_graph[task_idx_2],
        }

    def test_two_nccl_collectives(self, monkeypatch):
        """
        Simulate the case where there are two candidates that are NCCL collective
        operations. In this case, `_select_next_nodes` should return all the NCCL
        collective nodes that are bound earlier.

        driver -> fake_actor_1.allreduce_1 -> driver
               |                            |
               -> fake_actor_2.allreduce_1 ->
               |                            |
               -> fake_actor_3.allreduce_2 ->
               |                            |
               -> fake_actor_4.allreduce_2 ->
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1, task_idx_1, exec_task_idx_1 = ActorHandle("fake_actor_1"), 1, 0
        fake_actor_2, task_idx_2, exec_task_idx_2 = ActorHandle("fake_actor_2"), 2, 0
        fake_actor_3, task_idx_3, exec_task_idx_3 = ActorHandle("fake_actor_3"), 3, 0
        fake_actor_4, task_idx_4, exec_task_idx_4 = ActorHandle("fake_actor_4"), 4, 0

        nccl_op_1 = MockSyncGroup([task_idx_1, task_idx_2], {task_idx_1, task_idx_2})
        nccl_op_2 = MockSyncGroup([task_idx_3, task_idx_4], {task_idx_3, task_idx_4})

        mock_graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1,
                task_idx_1,
                fake_actor_1,
                nccl_op_1,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2,
                task_idx_2,
                fake_actor_2,
                nccl_op_1,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_3: generate_dag_graph_nodes(
                exec_task_idx_3,
                task_idx_3,
                fake_actor_3,
                nccl_op_2,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_4: generate_dag_graph_nodes(
                exec_task_idx_4,
                task_idx_4,
                fake_actor_4,
                nccl_op_2,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
        }
        mock_actor_to_candidates = {
            fake_actor_1: [mock_graph[task_idx_1]],
            fake_actor_2: [],
            fake_actor_3: [],
            fake_actor_4: [mock_graph[task_idx_4]],
        }

        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[task_idx_1],
            mock_graph[task_idx_2],
        }

        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert set(next_nodes) == {
            mock_graph[task_idx_3],
            mock_graph[task_idx_4],
        }

        next_nodes = _select_next_nodes(mock_actor_to_candidates, mock_graph)
        assert next_nodes is None


class TestBuildDAGNodeOperationGraph:
    """
    Test whether `_build_dag_node_operation_graph` function adds the correct
    edges between the nodes in the operation graph base on the 3 rules mentioned
    in the doc string of `_build_dag_node_operation_graph`.
    """

    def check_edge_between_compute_nodes(
        self,
        graph: Dict[int, _DAGOperationGraphNode],
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
        compute_node_1 = graph[task_idx_1]
        compute_node_2 = graph[task_idx_2]
        assert task_idx_2 in compute_node_1.out_edges
        assert task_idx_1 in compute_node_2.in_edges

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
            fake_actor_1: [generate_dag_graph_nodes(0, task_idx_1, fake_actor_1)],
            fake_actor_2: [generate_dag_graph_nodes(0, task_idx_2, fake_actor_2)],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

        self.check_edge_between_compute_nodes(graph, task_idx_1, task_idx_2)

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
                generate_dag_graph_nodes(0, task_idx_1, fake_actor),
                generate_dag_graph_nodes(1, task_idx_2, fake_actor),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 2

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
                generate_dag_graph_nodes(0, task_idx_1, fake_actor_1),
                generate_dag_graph_nodes(1, task_idx_3, fake_actor_1),
            ],
            fake_actor_2: [
                generate_dag_graph_nodes(0, task_idx_2, fake_actor_2),
                generate_dag_graph_nodes(1, task_idx_4, fake_actor_2),
            ],
        }
        graph = _build_dag_node_operation_graph(idx_to_task, actor_to_operation_nodes)
        assert len(graph) == 4

        self.check_edge_between_compute_nodes(graph, task_idx_1, task_idx_4)
        self.check_edge_between_compute_nodes(graph, task_idx_2, task_idx_3)


class TestGenerateActorToExecutionSchedule:
    """
    Test whether `_generate_actor_to_execution_schedule` function generates the
    correct execution schedule for each actor.
    """

    def add_data_dependeny(
        self,
        ops_writer: _DAGOperationGraphNode,
        ops_reader: _DAGOperationGraphNode,
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
        _add_edge(ops_writer, ops_reader)

    def add_control_dependency(
        self,
        ops_prev: _DAGOperationGraphNode,
        ops_next: _DAGOperationGraphNode,
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
        _add_edge(ops_prev, ops_next)

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
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_2])
        self.add_control_dependency(graph[task_idx_1], graph[task_idx_2])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 2
        assert actor_to_execution_schedule[fake_actor] == [
            graph[task_idx_1].op,
            graph[task_idx_2].op,
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
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_2])
        self.add_data_dependeny(graph[task_idx_1], graph[task_idx_3])
        self.add_control_dependency(graph[task_idx_1], graph[task_idx_2])
        self.add_control_dependency(graph[task_idx_2], graph[task_idx_3])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 1
        assert len(actor_to_execution_schedule[fake_actor]) == 3
        assert actor_to_execution_schedule[fake_actor] == [
            graph[task_idx_1].op,
            graph[task_idx_2].op,
            graph[task_idx_3].op,
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
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_control_dependency(graph[task_idx_2_1], graph[task_idx_2_2])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 2
        assert len(actor_to_execution_schedule[fake_actor_2]) == 2

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[task_idx_1_1].op,
            graph[task_idx_1_2].op,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            graph[task_idx_2_1].op,
            graph[task_idx_2_2].op,
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

        nccl_op_1 = MockSyncGroup([task_idx_1_1, task_idx_2_2])
        nccl_op_2 = MockSyncGroup([task_idx_2_1, task_idx_1_2])

        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1,
                task_idx_1_1,
                fake_actor_1,
                nccl_op_1,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1,
                task_idx_2_1,
                fake_actor_2,
                nccl_op_2,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2,
                task_idx_2_2,
                fake_actor_2,
                nccl_op_1,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2,
                task_idx_1_2,
                fake_actor_1,
                nccl_op_2,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
        }

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[fake_actor_1]) == 2
        assert len(actor_to_execution_schedule[fake_actor_2]) == 2

        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[task_idx_1_1].op,
            graph[task_idx_1_2].op,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            # The order of `task_idx_2_2.NCCL_READ` and `task_idx_2_2.COMPUTE`
            # is important.
            graph[task_idx_2_2].op,
            graph[task_idx_2_1].op,
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
        assert len(actor_to_execution_schedule[worker_1]) == 4
        assert len(actor_to_execution_schedule[worker_2]) == 4
        assert actor_to_execution_schedule[worker_1] == [
            graph[task_idx_1_1].op,
            graph[task_idx_1_2].op,
            graph[task_idx_1_3].op,
            graph[task_idx_1_4].op,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[task_idx_2_1].op,
            graph[task_idx_2_2].op,
            graph[task_idx_2_3].op,
            graph[task_idx_2_4].op,
        ]

    def test_simulate_pp_2workers_2batches_1f1b_with_nccl(self, monkeypatch):
        """
        This test simulates a simple 1F1B pipeline parallelism for training with
        2 workers and 2 batches.

        w1: fwd_b1 send fwd_b2 send   recv bwd_b1             recv bwd_b2
        w2:        recv fwd_b1 bwd_b1 send recv fwd_b2 bwd_b2 send

        The communication between workers is done using NCCL. The communication
        within the worker actor is done using IntraProcessChannel.
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        worker_1 = ActorHandle("worker_1")
        task_idx_1_1, exec_task_idx_1_1 = 1, 4
        task_idx_1_3, exec_task_idx_1_3 = 2, 5
        task_idx_1_6, exec_task_idx_1_6 = 3, 6
        task_idx_1_8, exec_task_idx_1_8 = 4, 7
        # worker 1 NCCL P2P operations.
        task_idx_1_2, exec_task_idx_1_2 = 9, 0
        task_idx_1_4, exec_task_idx_1_4 = 10, 1
        task_idx_1_5, exec_task_idx_1_5 = 11, 2
        task_idx_1_7, exec_task_idx_1_7 = 12, 3
        worker_2 = ActorHandle("worker_2")
        task_idx_2_2, exec_task_idx_2_2 = 5, 4
        task_idx_2_3, exec_task_idx_2_3 = 6, 5
        task_idx_2_6, exec_task_idx_2_6 = 7, 6
        task_idx_2_7, exec_task_idx_2_7 = 8, 7
        # worker 2 NCCL P2P operations.
        task_idx_2_1, exec_task_idx_2_1 = 13, 0
        task_idx_2_4, exec_task_idx_2_4 = 14, 1
        task_idx_2_5, exec_task_idx_2_5 = 15, 2
        task_idx_2_8, exec_task_idx_2_8 = 16, 3

        nccl_op_1 = MockSyncGroup([task_idx_1_2, task_idx_2_1])
        nccl_op_2 = MockSyncGroup([task_idx_1_4, task_idx_2_5])
        nccl_op_3 = MockSyncGroup([task_idx_2_4, task_idx_1_5])
        nccl_op_4 = MockSyncGroup([task_idx_2_8, task_idx_1_7])

        graph = {
            task_idx_1_1: generate_dag_graph_nodes(
                exec_task_idx_1_1,
                task_idx_1_1,
                worker_1,
            ),
            task_idx_1_3: generate_dag_graph_nodes(
                exec_task_idx_1_3,
                task_idx_1_3,
                worker_1,
            ),
            task_idx_1_6: generate_dag_graph_nodes(
                exec_task_idx_1_6,
                task_idx_1_6,
                worker_1,
            ),
            task_idx_1_8: generate_dag_graph_nodes(
                exec_task_idx_1_8,
                task_idx_1_8,
                worker_1,
            ),
            task_idx_2_2: generate_dag_graph_nodes(
                exec_task_idx_2_2,
                task_idx_2_2,
                worker_2,
            ),
            task_idx_2_3: generate_dag_graph_nodes(
                exec_task_idx_2_3,
                task_idx_2_3,
                worker_2,
            ),
            task_idx_2_6: generate_dag_graph_nodes(
                exec_task_idx_2_6,
                task_idx_2_6,
                worker_2,
            ),
            task_idx_2_7: generate_dag_graph_nodes(
                exec_task_idx_2_7,
                task_idx_2_7,
                worker_2,
            ),
            task_idx_1_2: generate_dag_graph_nodes(
                exec_task_idx_1_2,
                task_idx_1_2,
                worker_1,
                nccl_op_1,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_1_4: generate_dag_graph_nodes(
                exec_task_idx_1_4,
                task_idx_1_4,
                worker_1,
                nccl_op_2,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_1_5: generate_dag_graph_nodes(
                exec_task_idx_1_5,
                task_idx_1_5,
                worker_1,
                nccl_op_3,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
            task_idx_1_7: generate_dag_graph_nodes(
                exec_task_idx_1_7,
                task_idx_1_7,
                worker_1,
                nccl_op_4,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
            task_idx_2_1: generate_dag_graph_nodes(
                exec_task_idx_2_1,
                task_idx_2_1,
                worker_2,
                nccl_op_1,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
            task_idx_2_4: generate_dag_graph_nodes(
                exec_task_idx_2_4,
                task_idx_2_4,
                worker_2,
                nccl_op_3,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
            task_idx_2_5: generate_dag_graph_nodes(
                exec_task_idx_2_5,
                task_idx_2_5,
                worker_2,
                nccl_op_2,
                # requires_nccl_read=True,
                P2POp.RECV,
            ),
            task_idx_2_8: generate_dag_graph_nodes(
                exec_task_idx_2_8,
                task_idx_2_8,
                worker_2,
                nccl_op_4,
                # requires_nccl_write=True,
                P2POp.SEND,
            ),
        }
        self.add_data_dependeny(graph[task_idx_1_1], graph[task_idx_1_2])
        self.add_data_dependeny(graph[task_idx_1_3], graph[task_idx_1_4])
        self.add_data_dependeny(graph[task_idx_1_5], graph[task_idx_1_6])
        self.add_data_dependeny(graph[task_idx_1_7], graph[task_idx_1_8])
        self.add_data_dependeny(graph[task_idx_2_1], graph[task_idx_2_2])
        self.add_data_dependeny(graph[task_idx_2_3], graph[task_idx_2_4])
        self.add_data_dependeny(graph[task_idx_2_5], graph[task_idx_2_6])
        self.add_data_dependeny(graph[task_idx_2_7], graph[task_idx_2_8])
        self.add_control_dependency(graph[task_idx_1_1], graph[task_idx_1_3])
        self.add_control_dependency(graph[task_idx_1_3], graph[task_idx_1_6])
        self.add_control_dependency(graph[task_idx_1_6], graph[task_idx_1_8])
        self.add_control_dependency(graph[task_idx_2_2], graph[task_idx_2_3])
        self.add_control_dependency(graph[task_idx_2_3], graph[task_idx_2_6])
        self.add_control_dependency(graph[task_idx_2_6], graph[task_idx_2_7])

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert len(actor_to_execution_schedule[worker_1]) == 8
        assert len(actor_to_execution_schedule[worker_2]) == 8
        assert actor_to_execution_schedule[worker_1] == [
            graph[task_idx_1_1].op,
            graph[task_idx_1_2].op,
            graph[task_idx_1_3].op,
            graph[task_idx_1_4].op,
            graph[task_idx_1_5].op,
            graph[task_idx_1_6].op,
            graph[task_idx_1_7].op,
            graph[task_idx_1_8].op,
        ]
        assert actor_to_execution_schedule[worker_2] == [
            graph[task_idx_2_1].op,
            graph[task_idx_2_2].op,
            graph[task_idx_2_3].op,
            # The order of `task_idx_2_3.NCCL_READ` and `task_idx_2_2.NCCL_WRITE`
            # is important.
            graph[task_idx_2_5].op,
            graph[task_idx_2_4].op,
            graph[task_idx_2_6].op,
            graph[task_idx_2_7].op,
            graph[task_idx_2_8].op,
        ]

    def test_two_nccl_collectives(self, monkeypatch):
        """
        Simulate the case where there are two NCCL collective operations, one after
        the another.

        driver -> fake_actor_1.allreduce_1 -> fake_actor_1.allreduce_2 -> driver
               |                            |                           |
               -> fake_actor_2.allreduce_1 -> fake_actor_2.allreduce_2 ->
        """
        monkeypatch.setattr(ActorHandle, "__init__", mock_actor_handle_init)

        fake_actor_1 = ActorHandle("fake_actor_1")
        task_idx_1, exec_task_idx_1 = 1, 0
        task_idx_3, exec_task_idx_3 = 3, 1
        fake_actor_2 = ActorHandle("fake_actor_2")
        task_idx_2, exec_task_idx_2 = 2, 0
        task_idx_4, exec_task_idx_4 = 4, 1

        nccl_op_1 = MockSyncGroup([task_idx_1, task_idx_2])
        nccl_op_2 = MockSyncGroup([task_idx_3, task_idx_4])

        graph = {
            task_idx_1: generate_dag_graph_nodes(
                exec_task_idx_1,
                task_idx_1,
                fake_actor_1,
                nccl_op_1,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_2: generate_dag_graph_nodes(
                exec_task_idx_2,
                task_idx_2,
                fake_actor_2,
                nccl_op_1,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_3: generate_dag_graph_nodes(
                exec_task_idx_3,
                task_idx_3,
                fake_actor_1,
                nccl_op_2,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
            task_idx_4: generate_dag_graph_nodes(
                exec_task_idx_4,
                task_idx_4,
                fake_actor_2,
                nccl_op_2,
                # requires_nccl_collective=True,
                ReduceOp.SUM,
            ),
        }

        actor_to_execution_schedule = _generate_and_extract_execution_schedule(graph)
        assert len(actor_to_execution_schedule) == 2
        assert actor_to_execution_schedule[fake_actor_1] == [
            graph[task_idx_1].op,
            graph[task_idx_3].op,
        ]
        assert actor_to_execution_schedule[fake_actor_2] == [
            graph[task_idx_2].op,
            graph[task_idx_4].op,
        ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
