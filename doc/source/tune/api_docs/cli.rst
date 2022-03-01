Tune CLI (Experimental)
=======================

``tune`` has an easy-to-use command line interface (CLI) to manage and monitor your experiments on Ray.
To do this, verify that you have the ``tabulate`` library installed:

.. code-block:: bash

    $ pip install tabulate

Here is an example command line call:

``tune list-trials``: List tabular information about trials within an experiment.
Empty columns will be dropped by default. Add the ``--sort`` flag to sort the output by specific columns.
Add the ``--filter`` flag to filter the output in the format ``"<column> <operator> <value>"``.
Add the ``--output`` flag to write the trial information to a specific file (CSV or Pickle).
Add the ``--columns`` and ``--result-columns`` flags to select specific columns to display.

.. code-block:: bash

    $ tune list-trials [EXPERIMENT_DIR] --output note.csv

    +------------------+-----------------------+------------+
    | trainable_name   | experiment_tag        | trial_id   |
    |------------------+-----------------------+------------|
    | MyTrainableClass | 0_height=40,width=37  | 87b54a1d   |
    | MyTrainableClass | 1_height=21,width=70  | 23b89036   |
    | MyTrainableClass | 2_height=99,width=90  | 518dbe95   |
    | MyTrainableClass | 3_height=54,width=21  | 7b99a28a   |
    | MyTrainableClass | 4_height=90,width=69  | ae4e02fb   |
    +------------------+-----------------------+------------+
    Dropped columns: ['status', 'last_update_time']
    Please increase your terminal size to view remaining columns.
    Output saved at: note.csv

    $ tune list-trials [EXPERIMENT_DIR] --filter "trial_id == 7b99a28a"

    +------------------+-----------------------+------------+
    | trainable_name   | experiment_tag        | trial_id   |
    |------------------+-----------------------+------------|
    | MyTrainableClass | 3_height=54,width=21  | 7b99a28a   |
    +------------------+-----------------------+------------+
    Dropped columns: ['status', 'last_update_time']
    Please increase your terminal size to view remaining columns.
