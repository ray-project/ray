#
# SPDX-FileCopyrightText: Copyright (c) 1993-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from onnxslim.third_party.onnx_graphsurgeon.ir.graph import Graph


class BaseImporter:
    @staticmethod
    def import_graph(graph) -> Graph:
        """
        Import a graph from some source graph.

        Args:
            graph (object): The source graph to import. For example, this might be an onnx.GraphProto.

        Returns:
            Graph: The equivalent onnx-graphsurgeon graph.
        """
        raise NotImplementedError("BaseImporter is an abstract class")
