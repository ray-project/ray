Streaming Library
=================

Dependencies:

Install NetworkX: ``pip install networkx``

Examples:

- simple.py: A simple example with stateless operators and different parallelism per stage.

Run ``python simple.py --input-file toy.txt``

- wordcount.py: A streaming wordcount example with a stateful operator (rolling sum).

Run ``python wordcount.py --titles-file articles.txt``
