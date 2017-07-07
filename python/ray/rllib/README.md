# RLLib: Ray's scalable reinforcement learning library

## Getting Started

You can run training with
```
python train.py --env CartPole-v0 --alg PolicyGradient --s3-bucket s3://rllib
```

The available algorithms are:

* `PolicyGradient` is a proximal variant of [TRPO](https://arxiv.org/abs/1502.05477).

* `EvolutionStrategies` is decribed in [this paper](https://arxiv.org/abs/1703.03864). Our implementation borrows code from
[here](https://github.com/openai/evolution-strategies-starter).

* `DQN` is an implementation of [Deep Q Networks](https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf) based on [OpenAI baselines](https://github.com/openai/baselines).

* `A3C` is an implementation of [A3C](https://arxiv.org/abs/1602.01783) based on [the OpenAI starter agent](https://github.com/openai/universe-starter-agent).

## Storing logs in Amazon S3

If you pass in the `--s3-bucket s3://bucketname` flag into `train.py`, the
script will will upload statistics about the training run to S3. These
can be queried with Athena. First create tables containing the experimental
results with

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS experiments (
  experiment_id STRING,
  env_name STRING,
  alg STRING,
  -- result.json
  training_iteration INT,
  episode_reward_mean FLOAT,
  episode_len_mean FLOAT
) ROW FORMAT serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://rllib/'
```

and then you can for example visualize the results with

```sql
SELECT c.experiment_id, c.env_name, c.alg, a.episode_reward_mean, a.episode_len_mean
FROM experiments a
LEFT OUTER JOIN experiments b
    ON a.experiment_id = b.experiment_id AND a.training_iteration < b.training_iteration
INNER JOIN experiments c
    ON a.experiment_id = c.experiment_id
WHERE b.experiment_id IS NULL AND a.training_iteration IS NOT NULL AND c.alg is NOT NULL;
```

This query selects last iteration from each experiment (see [this stackoverflow post](https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column)).
