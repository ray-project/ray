News Reader
===========

This example shows how to implement a simple news reader using Ray. The RSS
feed reader app consists of a simple ``Vue.js`` `frontend`_ and a backend 
consisting of a Flask server and a Ray actor. View the `code for this 
example`_.

To run this example, you will need to install `NPM`_ and a few python dependencies.

.. code-block:: bash

  pip install atoma
  pip install flask
  pip install flask-cors


Navigate to the ``ray/doc/source/ray-core/_examples/newsreader`` directory and start the Flask server.

.. code-block:: bash

  python server.py

Clone the client code.

.. code-block:: bash

  git clone https://github.com/ray-project/qreader

Start the client.

.. code-block:: bash

  cd qreader;  npm install; npm run dev

Once the ``qreader`` is running, you can visit the dashboard which should open 
once its running. If it fails to open on its own, you can visit it at 
the url listed in the screen output. 

You can now add a channel by clicking "Add channel". For example, try
pasting ``http://news.ycombinator.com/rss`` into the field.

Star some of the articles in the channel. Each time you star an article, you 
can see the Flask server responding in its terminal. 

Now we will view our database. Navigate back to the 
``ray/doc/source/ray-core/_examples/newsreader`` directory. Access the database by running
``sqlite3 newsreader.db`` in the terminal. This will start a sqlite session.
View all the articles in the ``news`` table by running ``SELECT * FROM news;``.
For more details on commands in sqlite, you can run `.help` in 
the database.

.. _`frontend`: https://github.com/saqueib/qreader
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/source/ray-core/_examples/newsreader
.. _`NPM`: https://docs.npmjs.com/downloading-and-installing-node-js-and-npm
