Add Protobuf Files
=========================

``.proto`` files are located in the ``src/ray/protobuf`` folder.

If you add new protobuf files such as ``src/ray/protobuf/hello.proto``, you need to add a ``proto_library`` rule to the ``src/ray/protobuf/BUILD`` file. If you want to generate C++ files, you also need to add a ``cc_proto_library`` rule. If you want to generate Python files, you need to add a ``python_grpc_compile`` rule.

If your ``python_grpc_compile`` rule is not transitively included in the ``all_py_proto`` filegroup, you need to add it manually to the ``all_py_proto`` filegroup in ``BUILD.bazel``.

If you add new protobuf files in a nested folder, you should prefix all files in that folder with a unique prefix such as ``my_unique_prefix``. For example: ``src/ray/protobuf/nested_folder/my_unique_prefix_hello.proto``. Nested folder structures for protobuf files are currently not supported, so you cannot create files like ``src/ray/protobuf/first_level/second_level/hello.proto``.

When you have profobuf files in a nested folder, you also need to add a ``sed`` command to the ``install_py_proto`` rule in ``BUILD.bazel``. Otherwise, the Python import paths will be incorrect.

.. code-block:: shell

    my_unique_prefix_files=(`ls python/ray/core/generated/my_unique_prefix*_pb2*.py`)
    sed -i -E 's/from ..my_unique_prefix/from ./' "$${my_unique_prefix_files[@]}"
