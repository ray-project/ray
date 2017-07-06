
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS experiments (
  experiment_id STRING,
  -- result.json
  training_iteration INT,
  episode_reward_mean FLOAT,
  episode_len_mean FLOAT,
  -- info.json
  kl_divergence FLOAT,
  kl_coefficient FLOAT,
  checkpointing_time FLOAT,
  rollouts_time FLOAT,
  shuffle_time FLOAT,
  load_time FLOAT,
  sgd_time FLOAT,
  sample_throughput FLOAT,
  -- config.json
  sgd_stepsize FLOAT,
  num_agents INT,
  sgd_batchsize INT
) ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://rllib/'
```

```sql
SELECT * FROM experiments WHERE num_agents IS NOT NULL

SELECT sgd_stepsize, sgd_batchsize FROM experiments WHERE num_agents IS NOT NULL
```

```sql
-- https://stackoverflow.com/questions/7745609/sql-select-only-rows-with-max-value-on-a-column
SELECT a.episode_reward_mean, a.episode_len_mean
FROM experiments a
LEFT OUTER JOIN experiments b
    ON a.experiment_id = b.experiment_id AND a.training_iteration < b.training_iteration
WHERE b.experiment_id IS NULL AND a.training_iteration IS NOT NULL;
```
