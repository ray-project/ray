
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
