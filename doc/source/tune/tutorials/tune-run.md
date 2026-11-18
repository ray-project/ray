---
myst:
  html_meta:
    description: "Run basic Tune experiments with independent trials in parallel, and compare Tune with writing the same sweep with Ray Core tasks."
---

(tune-parallel-experiments-guide)=

# Run basic Tune experiments

The most common and simplest way to use Ray Tune is as a parallel experiment runner. If you can define your trials in a Python function, you can use Tune to run hundreds to thousands of independent trials in a cluster. Tune manages trial execution, status reporting, and fault tolerance.

## Run independent trials in parallel

The following example uses Tune to run `N` independent model training trials as a grid sweep. Each trial can run different code depending on the configuration dictionary passed to it.

**Step 1:** Define the model training function that you want to run variations of. The function takes a configuration dictionary as an argument and returns a dictionary. To learn more about logging Tune results, see {ref}`tune-logging`.

```{literalinclude} ../doc_code/tune.py
:language: python
:start-after: __step1_begin__
:end-before: __step1_end__
```

**Step 2:** Define the space of trials to run. This example defines a grid sweep from `0..NUM_MODELS`, which generates the configuration dictionaries that Tune passes to the training function. To learn about the features Tune offers for defining search spaces, see {ref}`tune-search-space-tutorial`.

```{literalinclude} ../doc_code/tune.py
:language: python
:start-after: __step2_begin__
:end-before: __step2_end__
```

**Step 3:** Optionally, configure the resources to allocate to each trial. Tune uses this allocation to control parallelism. For example, if you configure each trial to use 4 CPUs and the cluster has only 32 CPUs, Tune limits the number of concurrent trials to 8 to avoid overloading the cluster. For more information, see {ref}`tune-parallelism`.

```{literalinclude} ../doc_code/tune.py
:language: python
:start-after: __step3_begin__
:end-before: __step3_end__
```

**Step 4:** Run the experiment with Tune. Tune reports the experiment status, and you can inspect the results after the experiment finishes. Tune can automatically retry failed trials as well as entire experiments. For more information, see {ref}`tune-stopping-guide`.

```{literalinclude} ../doc_code/tune.py
:language: python
:start-after: __step4_begin__
:end-before: __step4_end__
```

**Step 5:** Inspect the results. Until the experiment finishes, Tune periodically prints a summary of its status to stdout, similar to the following:

```text
== Status ==
Current time: 2022-09-21 10:19:34 (running for 00:00:04.54)
Memory usage on this node: 6.9/31.1 GiB
Using FIFO scheduling algorithm.
Resources requested: 0/8 CPUs, 0/0 GPUs, 0.0/16.13 GiB heap, 0.0/8.06 GiB objects
Result logdir: /home/ubuntu/ray_results/train_model_2022-09-21_10-19-26
Number of trials: 100/100 (100 TERMINATED)
+-------------------------+------------+----------------------+------------+--------+------------------+
| Trial name              | status     | loc                  | model_id   |   iter |   total time (s) |
|-------------------------+------------+----------------------+------------+--------+------------------|
| train_model_8d627_00000 | TERMINATED | 192.168.1.67:2381731 | model_0    |      1 |      8.46386e-05 |
| train_model_8d627_00001 | TERMINATED | 192.168.1.67:2381761 | model_1    |      1 |      0.000126362 |
| train_model_8d627_00002 | TERMINATED | 192.168.1.67:2381763 | model_2    |      1 |      0.000112772 |
...
| train_model_8d627_00097 | TERMINATED | 192.168.1.67:2381731 | model_97   |      1 |      5.57899e-05 |
| train_model_8d627_00098 | TERMINATED | 192.168.1.67:2381767 | model_98   |      1 |      6.05583e-05 |
| train_model_8d627_00099 | TERMINATED | 192.168.1.67:2381763 | model_99   |      1 |      6.69956e-05 |
+-------------------------+------------+----------------------+------------+--------+------------------+

2022-09-21 10:19:35,159	INFO tune.py:762 -- Total run time: 5.06 seconds (4.46 seconds for the tuning loop).
```

The final result objects contain metadata for the finished trials:

```text
Result(metrics={'score': 'model_0', 'other_data': Ellipsis, 'done': True, 'trial_id': '8d627_00000', 'experiment_tag': '0_model_id=model_0'}, error=None, log_dir=PosixPath('/home/ubuntu/ray_results/train_model_2022-09-21_10-19-26/train_model_8d627_00000_0_model_id=model_0_2022-09-21_10-19-30'))
Result(metrics={'score': 'model_1', 'other_data': Ellipsis, 'done': True, 'trial_id': '8d627_00001', 'experiment_tag': '1_model_id=model_1'}, error=None, log_dir=PosixPath('/home/ubuntu/ray_results/train_model_2022-09-21_10-19-26/train_model_8d627_00001_1_model_id=model_1_2022-09-21_10-19-31'))
Result(metrics={'score': 'model_2', 'other_data': Ellipsis, 'done': True, 'trial_id': '8d627_00002', 'experiment_tag': '2_model_id=model_2'}, error=None, log_dir=PosixPath('/home/ubuntu/ray_results/train_model_2022-09-21_10-19-26/train_model_8d627_00002_2_model_id=model_2_2022-09-21_10-19-31'))
```

## How does Tune compare to Ray Core tasks?

You can also run trials in parallel with Ray Core {ref}`tasks <ray-remote-functions>`. For example, you could rewrite the preceding example with `ray.remote` as follows:

```{literalinclude} ../doc_code/tune.py
:language: python
:start-after: __tasks_begin__
:end-before: __tasks_end__
```

Compared to Ray tasks, Tune offers the following additional features:

* Status reporting and tracking, including integrations and callbacks to common monitoring tools
* Checkpointing of trials for fine-grained fault tolerance
* Gang scheduling of multi-worker trials

Consider using Tune if you need status tracking or support for more advanced ML workloads.
