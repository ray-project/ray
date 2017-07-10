RLLib: Ray's modular and scalable reinforcement learning library
================================================================

Getting Started
---------------

You can run training with

::

    python train.py --env CartPole-v0 --alg PolicyGradient

The available algorithms are:

-  ``PolicyGradient`` is a proximal variant of
   `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  ``EvolutionStrategies`` is decribed in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   borrows code from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

-  ``DQN`` is an implementation of `Deep Q
   Networks <https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf>`__ based on
   `OpenAI baselines <https://github.com/openai/baselines>`__.

-  ``A3C`` is an implementation of
   `A3C <https://arxiv.org/abs/1602.01783>`__ based on `the OpenAI
   starter agent <https://github.com/openai/universe-starter-agent>`__.

Storing logs
------------

You can store the algorithm configuration (including hyperparameters) and
training results on a filesystem with the ``--upload-dir`` flag. Two protocols
are supported at the moment:

- ``--upload-dir file:///tmp/ray/`` will store the logs on the local filesystem
  in a subdirectory of /tmp/ray which is named after the algorithm name, the
  environment and the current date. This is the default.

- ``--upload-dir s3://bucketname/`` will store the logs in S3. Not that if you
  store the logs in S3, TensorFlow files will not currently be stored because
  TensorFlow doesn't support directly uploading files to S3 at the moment.

Querying logs with Athena
-------------------------

If you stored the logs in S3 or uploaded them there from the local file system,
they can be queried with Athena. First create tables containing the
experimental results with

.. code:: sql

    CREATE EXTERNAL TABLE IF NOT EXISTS experiments (
      experiment_id STRING,
      env_name STRING,
      alg STRING,
      -- result.json
      training_iteration INT,
      episode_reward_mean FLOAT,
      episode_len_mean FLOAT
    ) ROW FORMAT serde 'org.apache.hive.hcatalog.data.JsonSerDe'
    LOCATION 's3://bucketname/'

and then you can for example visualize the results with

.. code:: sql

    SELECT c.experiment_id, c.env_name, c.alg, a.episode_reward_mean, a.episode_len_mean
    FROM experiments a
    LEFT OUTER JOIN experiments b
        ON a.experiment_id = b.experiment_id AND a.training_iteration < b.training_iteration
    INNER JOIN experiments c
        ON a.experiment_id = c.experiment_id
    WHERE b.experiment_id IS NULL AND a.training_iteration IS NOT NULL AND c.alg is NOT NULL;

This query selects last iteration from each experiment (see `this
stackoverflow
post <https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column>`__).
