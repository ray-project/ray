News Reader
===========

This document shows how to implement a simple news reader using Ray. The reader
consists of a simple Vue.js `frontend`_ and a backend consisting of a Flask
server and a Ray actor. View the `code for this example`_.

To run this example, you will need to install NPM and a few python dependencies.

.. code-block:: bash

  pip install atoma
  pip install flask


To use this example you need to

* In the ``ray/doc/examples/newsreader`` directory, start the server with
  ``python server.py``.
* Clone the client code with ``git clone https://github.com/ray-project/qreader``
* Start the client with ``cd qreader;  npm install; npm run dev``
* You can now add a channel by clicking "Add channel" and for example pasting
  ``http://news.ycombinator.com/rss`` into the field.
* Star some of the articles and dump the database by running
  ``sqlite3 newsreader.db`` in a terminal in the ``ray/doc/examples/newsreader``
  directory and entering ``SELECT * FROM news;``.

.. _`frontend`: https://github.com/saqueib/qreader
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/examples/newsreader
