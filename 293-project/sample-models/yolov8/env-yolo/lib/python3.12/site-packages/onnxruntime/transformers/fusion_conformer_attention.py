# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation.  All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------
import logging

from fusion_attention import AttentionMask, FusionAttention
from onnx_model import OnnxModel

logger = logging.getLogger(__name__)


class FusionConformerAttention(FusionAttention):
    """
    Fuse Conformer Attention subgraph into one MultiHeadAttention node.
    """

    def __init__(
        self,
        model: OnnxModel,
        hidden_size: int,
        num_heads: int,
        attention_mask: AttentionMask,
    ):
        super().__init__(model, hidden_size, num_heads, attention_mask)

    def fuse(self, normalize_node, input_name_to_nodes, output_name_to_node):
        # SkipLayerNormalization has two inputs, and one of them is the root input for attention.
        qkv_nodes = self.model.match_parent_path(
            normalize_node,
            ["Add", "MatMul", "Reshape", "Transpose", "MatMul"],
            [1, 1, 0, 0, 0],
        )
        if qkv_nodes is not None:
            (
                _,
                _,
                reshape_qkv,
                transpose_qkv,
                matmul_qkv,
            ) = qkv_nodes
        else:
            logger.debug("fuse_conformer_attention: failed to match qkv path")
            return

        v_nodes = self.model.match_parent_path(
            matmul_qkv,
            ["Concat", "Transpose", "Reshape", "Add", "MatMul"],
            [1, 1, 0, 0, 1],
        )

        add_v = None
        if v_nodes is not None:
            (concat_v, _, _, add_v, matmul_v) = v_nodes
            concat_parent = self.model.get_parent(concat_v, 0, None)
            present_v = concat_v.output[0]
            past_v = concat_parent.output[0]
        else:
            logger.debug("fuse_conformer_attention: failed to match v path")
            return

        qk_nodes = self.model.match_parent_path(matmul_qkv, ["Softmax", "Add", "MatMul"], [0, 0, 0])

        if qk_nodes is not None:
            _, add_qk, matmul_qk = qk_nodes
        else:
            logger.debug("fuse_conformer_attention: failed to match qk path")
            return

        q_nodes = self.model.match_parent_path(
            matmul_qk,
            ["Div", "Transpose", "Reshape", "Add", "MatMul"],
            [0, 0, 0, 0, 1],
        )
        if q_nodes is not None:
            _, _, reshape_q, add_q, matmul_q = q_nodes
        else:
            logger.debug("fuse_conformer_attention: failed to match q path")
            return

        k_nodes = self.model.match_parent_path(
            matmul_qk,
            ["Transpose", "Concat", "Transpose", "Reshape", "Add", "MatMul"],
            [1, 0, 1, 0, 0, 1],
        )

        matmul_k = None
        if k_nodes is not None:
            _, concat_k, _, _, add_k, matmul_k = k_nodes
            concat_parent = self.model.get_parent(concat_k, 0, None)
            past_k = concat_parent.output[0]
            present_k = concat_k.output[0]
        else:
            logger.debug("fuse_conformer_attention: failed to match k path")
            return

        attention_last_node = reshape_qkv
        num_heads, hidden_size = self.get_num_heads_and_hidden_size(reshape_q)

        if num_heads <= 0 or hidden_size <= 0 or (hidden_size % num_heads) != 0:
            logger.debug("fuse_conformer_attention: failed to detect num_heads or hidden_size")
            return

        new_node = self.create_multihead_attention_node(
            matmul_q,
            matmul_k,
            matmul_v,
            add_q,
            add_k,
            add_v,
            num_heads,
            hidden_size,
            attention_last_node.output[0],
            add_qk=add_qk.input[1],
            past_k=past_k,
            past_v=past_v,
            present_k=present_k,
            present_v=present_v,
        )

        if new_node is None:
            logger.debug("fuse_conformer_attention: MultiHeadAttention node creation failed")
            return

        self.nodes_to_add.append(new_node)
        self.node_name_to_graph_name[new_node.name] = self.this_graph_name

        self.nodes_to_remove.extend([attention_last_node, transpose_qkv, matmul_qkv])
        self.nodes_to_remove.extend(qk_nodes)

        # When using multihead attention, keep MatMul nodes in original graph
        if q_nodes[-1].op_type == "MatMul":
            q_nodes.pop()
        if k_nodes[-1].op_type == "MatMul":
            k_nodes.pop()
        if v_nodes[-1].op_type == "MatMul":
            v_nodes.pop()

        self.nodes_to_remove.extend(k_nodes)
        self.nodes_to_remove.extend(v_nodes)

        # Use prune graph to remove mask nodes since they are shared by all attention nodes.
        self.prune_graph = True
