.. _data-representations:

Data Representations
====================

.. currentmodule:: ray.data

.. _block-api:

Block API
---------

.. autosummary::
   :toctree: doc/

   block.Block
   block.BlockExecStats
   block.BlockMetadata
   block.BlockAccessor

Batch API
---------

.. autosummary::
   :toctree: doc/
   
   block.DataBatch

Row API
--------

.. autosummary::
   :toctree: doc/

   row.TableRow

.. _dataset-tensor-extension-api:

Tensor Column Extension API
---------------------------

.. autosummary::
   :toctree: doc/

   extensions.tensor_extension.TensorDtype
   extensions.tensor_extension.TensorArray
   extensions.tensor_extension.ArrowTensorType
   extensions.tensor_extension.ArrowTensorArray
   extensions.tensor_extension.ArrowVariableShapedTensorType
   extensions.tensor_extension.ArrowVariableShapedTensorArray

