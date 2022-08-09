

# how to run 

- set up the tpu pod 

```python
bash tpu_launcher.sh
```

- test the tpu 

```python
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo python3 -c \"import jax; print(jax.device_count(), jax.local_device_count())\"" --worker all
```

- upload the file 

```python
bash sync_up_jax_dataset.sh
```


# Trouble shooting 

- error with the ssh 

```python
jimmy@104.154.245.121: Permission denied (publickey).
Failed to execute command on multiple workers. This may have happened if you have not added your SSH key to your ssh-agent using "ssh-add ~/.ssh/google_compute_engine".
Retrying: SSH command error: [/usr/bin/ssh] exited with return code [255].
jimmy@34.134.7.67: Permission denied (publickey).
Failed to execute command on multiple workers. This may have happened if you have not added your SSH key to your ssh-agent using "ssh-add ~/.ssh/google_compute_engine".
Retrying: SSH command error: [/usr/bin/ssh] exited with return code [255].
jimmy@35.239.84.164: Permission denied (publickey).
Failed to execute command on multiple workers. This may have happened if you have not added your SSH key to your ssh-agent using "ssh-add ~/.ssh/google_compute_engine".
Retrying: SSH command error: [/usr/bin/ssh] exited with return code [255].
jimmy@104.154.185.184: Permission denied (publickey).
Failed to execute command on multiple workers. This may have happened if you have not added your SSH key to your ssh-agent using "ssh-add ~/.ssh/google_compute_engine".
Retrying: SSH command error: [/usr/bin/ssh] exited with return code [255].
```

Solution: [link](https://stackoverflow.com/questions/26193535/error-gcloud-compute-ssh-usr-bin-ssh-exited-with-return-code-255)

```python 
sudo gcloud compute config-ssh
```


- error with the guestid

```python
INVALID_ARGUMENT: Cloud TPU received an invalid argument. The "GuestAttributes" value "" was not found.
```

Solution: 

```python
gcloud auth login
```


- error with the scp in tpu 

build the temp bridge between the local machine and the tpu

```bash
bash sync_up_jax_dataset.py
```

- error with torch, [link](https://stackoverflow.com/questions/67257008/oserror-libmkl-intel-lp64-so-1-cannot-open-shared-object-file-no-such-file-or)

```
>>> import torch
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/local/lib/python3.8/dist-packages/torch/__init__.py", line 198, in <module>
    _load_global_deps()
  File "/usr/local/lib/python3.8/dist-packages/torch/__init__.py", line 151, in _load_global_deps
    ctypes.CDLL(lib_path, mode=ctypes.RTLD_GLOBAL)
  File "/usr/lib/python3.8/ctypes/__init__.py", line 373, in __init__
    self._handle = _dlopen(self._name, mode)
OSError: libmkl_intel_lp64.so.1: cannot open shared object file: No such file or directory
```

Solution: sudo with env, and launch ray with env variables!

```
sudo env LD_LIBRARY_PATH=/usr/local/lib python3 test_ray_data set_remote_jax_ttexample.py
```

and 

```python
head_ip=`gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo python3 -c \"import ray; print(ray._private.services.get_node_ip_address())\"" --worker 0`
echo "head node ip: "$head_ip

gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo ray stop && sudo env LD_LIBRARY_PATH=/usr/local/lib ray start --head --port=6379 --resources='{\"TPU\":1}'" --worker=0

for i in 1 2 3 
do 
    gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo ray stop && sudo env LD_LIBRARY_PATH=/usr/local/lib ray start --address='${head_ip}:6379' --resources='{\"TPU\":1}'" --worker=$i
done 
```

- ray files error  (fixed!)

```python
g trials). If you're running an experiment with a large number of trials, this could lead to scheduling overhead. In this case, consider setting the `TUNE_MAX_PENDING_TRIALS_PG` environment variable to the desired maximum number of concurrent trials.
Traceback (most recent call last):
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/tuner.py", line 191, in fit
    return self._local_tuner.fit()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/impl/tuner_internal.py", line 158, in fit
    analysis = self._fit_internal(trainable, param_space)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/impl/tuner_internal.py", line 166, in _fit_internal
    analysis = run(
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/tune.py", line 723, in run
    runner.step()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/trial_runner.py", line 774, in step
    next_trial = self._update_trial_queue_and_get_next_trial()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/trial_runner.py", line 710, in _update_trial_queue_and_get_next_trial
    if not self._update_trial_queue(blocking=wait_for_trial):
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/trial_runner.py", line 1295, in _update_trial_queue
    trial = self._search_alg.next_trial()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/suggest/basic_variant.py", line 362, in next_trial
    trial = next(self._trial_iter)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/suggest/basic_variant.py", line 179, in __next__
    return self.create_trial(resolved_vars, spec)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/suggest/basic_variant.py", line 124, in create_trial
    return create_trial_from_spec(
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/config_parser.py", line 225, in create_trial_from_spec
    return Trial(
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/trial.py", line 292, in __init__
    default_resources = trainable_cls.default_resource_request(self.config)
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 387, in default_resource_request
    trainer_cls._validate_and_get_scaling_config_data_class(
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 224, in _validate_and_get_scaling_config_data_class
    ensure_only_allowed_dict_keys_set(
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/utils/config.py", line 21, in ensure_only_allowed_dict_keys_set
    raise ValueError(
ValueError: Key(s) ['resources_per_worker'] are not allowed to be set in the current context. Remove them from the dict.

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "test_ray_dataset_remote_jax_ttexample.py", line 57, in <module>
    result = trainer.fit()
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 327, in fit
    result_grid = tuner.fit()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/tuner.py", line 193, in fit
    raise TuneError(
ray.tune.error.TuneError: Tune run failed. Please use tuner = Tuner.restore("/root/ray_results/JaxTrainer_2022-06-03_01-27-03") to resume.
```







### test and output log


#### test1
```python
(base) ~ gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "cd ~/test_trainer && sudo env LD_LIBRARY_PATH=/usr/local/lib python3 test_ray_dataset_remote_jax_ttexample.py" --worker 0
```


```python
jimmy@t1v-n-e8b2b80d-w-0:~/test_trainer$ sudo env LD_LIBRARY_PATH=/usr/local/lib python3  test_ray_dataset_remote_jax_ttexample.py
2022-06-03 17:35:44,372	WARNING trial_runner.py:297 -- The maximum number of pending trials has been automatically set to the number of available cluster CPUs, which is high (422 CPUs/pending trials). If you're running an experiment with a large number of trials, this could lead to scheduling overhead. In this case, consider setting the `TUNE_MAX_PENDING_TRIALS_PG` environment variable to the desired maximum number of concurrent trials.
(raylet, ip=10.128.15.201) 2022-06-03 17:35:45,343	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.201 --node-manager-port=32831 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=63871 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=0 --runtime-env-hash=-1022478060
== Status ==
Current time: 2022-06-03 17:35:46 (running for 00:00:02.48)
Memory usage on this node: 4.2/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_98380_00000 | RUNNING  | 10.128.15.201:15613 |
+------------------------+----------+---------------------+


(raylet, ip=10.128.15.201) 2022-06-03 17:35:47,245	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.201 --node-manager-port=32831 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=63871 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=1 --runtime-env-hash=-1022478060
(raylet, ip=10.128.15.200) 2022-06-03 17:35:47,265	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.200 --node-manager-port=39403 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=64639 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=0 --runtime-env-hash=-1022478060
(raylet) 2022-06-03 17:35:47,256	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.198 --node-manager-port=43987 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=62164 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=23 --runtime-env-hash=-1022478060
(raylet, ip=10.128.15.199) 2022-06-03 17:35:47,280	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.199 --node-manager-port=45799 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=62486 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=0 --runtime-env-hash=-1022478060
== Status ==
Current time: 2022-06-03 17:35:52 (running for 00:00:07.77)
Memory usage on this node: 4.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_98380_00000 | RUNNING  | 10.128.15.201:15613 |
+------------------------+----------+---------------------+


== Status ==
Current time: 2022-06-03 17:35:57 (running for 00:00:12.92)
Memory usage on this node: 4.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_98380_00000 | RUNNING  | 10.128.15.201:15613 |
+------------------------+----------+---------------------+


== Status ==
Current time: 2022-06-03 17:36:02 (running for 00:00:17.93)
Memory usage on this node: 4.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_98380_00000 | RUNNING  | 10.128.15.201:15613 |
+------------------------+----------+---------------------+


(BaseWorkerMixin pid=15650, ip=10.128.15.201) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=46941) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=40765, ip=10.128.15.200) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=15481, ip=10.128.15.199) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=46941) 50
(BaseWorkerMixin pid=46941) 5
== Status ==
Current time: 2022-06-03 17:36:07 (running for 00:00:22.93)
Memory usage on this node: 5.5/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_98380_00000 | RUNNING  | 10.128.15.201:15613 |
+------------------------+----------+---------------------+


(BaseWorkerMixin pid=15650, ip=10.128.15.201) 50
(BaseWorkerMixin pid=15650, ip=10.128.15.201) 5
(BaseWorkerMixin pid=40765, ip=10.128.15.200) 50
(BaseWorkerMixin pid=40765, ip=10.128.15.200) 5
(BaseWorkerMixin pid=15481, ip=10.128.15.199) 50
(BaseWorkerMixin pid=15481, ip=10.128.15.199) 5
Trial JaxTrainer_98380_00000 completed. Last result:
== Status ==
Current time: 2022-06-03 17:36:11 (running for 00:00:27.38)
Memory usage on this node: 5.5/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (0.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-35-43
Number of trials: 1/1 (1 TERMINATED)
+------------------------+------------+---------------------+
| Trial name             | status     | loc                 |
|------------------------+------------+---------------------|
| JaxTrainer_98380_00000 | TERMINATED | 10.128.15.201:15613 |
+------------------------+------------+---------------------+


2022-06-03 17:36:11,852	INFO tune.py:752 -- Total run time: 28.19 seconds (27.37 seconds for the tuning loop).
```



But this fails!
```python
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "sudo env LD_LIBRARY_PATH=/usr/local/lib python3  test_ray_dataset_remote_jax_ttexample.py" --worker 0
```

the output is 
```python
2022-06-03 17:42:13,264	ERROR tune.py:748 -- Trials did not complete: [JaxTrainer_7da2e_00000]
2022-06-03 17:42:13,264	INFO tune.py:752 -- Total run time: 4.70 seconds (4.06 seconds for the tuning loop).
Traceback (most recent call last):
  File "/home/jimmy/test_trainer/test_ray_dataset_remote_jax_ttexample.py", line 61, in <module>
    result = trainer.fit()
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 332, in fit
    raise result.error
ray.exceptions.RayTaskError(ModuleNotFoundError): ray::TrainTrainable.train() (pid=42109, ip=10.128.15.200, repr=JaxTrainer)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/trainable.py", line 360, in train
    result = self.step()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/function_runner.py", line 404, in step
    self._report_thread_runner_error(block=True)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/function_runner.py", line 574, in _report_thread_runner_error
    raise e
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/function_runner.py", line 277, in run
    self._entrypoint()
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/function_runner.py", line 349, in entrypoint
    return self._trainable_func(
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 381, in _trainable_func
    super()._trainable_func(self._merged_config, reporter, checkpoint_dir)
  File "/usr/local/lib/python3.8/dist-packages/ray/tune/function_runner.py", line 645, in _trainable_func
    output = fn()
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/trainer.py", line 356, in train_func
    trainer.training_loop()
  File "/usr/local/lib/python3.8/dist-packages/ray/ml/train/data_parallel_trainer.py", line 355, in training_loop
    for results in training_iterator:
  File "/usr/local/lib/python3.8/dist-packages/ray/train/trainer.py", line 753, in __next__
    self._final_results = self._run_with_error_handling(
  File "/usr/local/lib/python3.8/dist-packages/ray/train/trainer.py", line 714, in _run_with_error_handling
    return func()
  File "/usr/local/lib/python3.8/dist-packages/ray/train/trainer.py", line 825, in _finish_training
    return self._backend_executor.finish_training()
  File "/usr/local/lib/python3.8/dist-packages/ray/train/backend.py", line 498, in finish_training
    results = self.get_with_failure_handling(futures)
  File "/usr/local/lib/python3.8/dist-packages/ray/train/backend.py", line 517, in get_with_failure_handling
    success = check_for_failure(remote_values)
  File "/usr/local/lib/python3.8/dist-packages/ray/train/utils.py", line 50, in check_for_failure
    ray.get(object_ref)
ray.exceptions.RayTaskError(ModuleNotFoundError): ray::BaseWorkerMixin._BaseWorkerMixin__execute() (pid=42142, ip=10.128.15.200, repr=<ray.train.worker_group.BaseWorkerMixin object at 0x7eef6181d610>)
  File "/usr/local/lib/python3.8/dist-packages/ray/train/worker_group.py", line 26, in __execute
    return func(*args, **kwargs)
  File "/usr/local/lib/python3.8/dist-packages/ray/train/backend.py", line 489, in end_training
    output = session.finish()
  File "/usr/local/lib/python3.8/dist-packages/ray/train/session.py", line 118, in finish
    func_output = self.training_thread.join()
  File "/usr/local/lib/python3.8/dist-packages/ray/train/utils.py", line 96, in join
    raise self.exc
  File "/usr/local/lib/python3.8/dist-packages/ray/train/utils.py", line 89, in run
    self.ret = self._target(*self._args, **self._kwargs)
  File "/usr/local/lib/python3.8/dist-packages/ray/train/utils.py", line 138, in <lambda>
    return lambda: train_func(config)
  File "/home/jimmy/test_trainer/test_ray_dataset_remote_jax_ttexample.py", line 30, in worker
    feature_columns=["x"],
ModuleNotFoundError: No module named 'jax.numpy'; 'jax' is not a package
```


Also, if i set the worker to be 2

```python
(BaseWorkerMixin pid=45527, ip=10.128.15.199) /usr/local/lib/python3.8/dist-packages/jax/_src/lib/xla_bridge.py:172: UserWarning: TPU backend initialization is taking more than 60.0 seconds. Did you run your code on all TPU hosts? See https://jax.readthedocs.io/en/latest/multi_process.html for more information.
(BaseWorkerMixin pid=45527, ip=10.128.15.199)   warnings.warn(
(BaseWorkerMixin pid=51669) /usr/local/lib/python3.8/dist-packages/jax/_src/lib/xla_bridge.py:172: UserWarning: TPU backend initialization is taking more than 60.0 seconds. Did you run your code on all TPU hosts? See https://jax.readthedocs.io/en/latest/multi_process.html for more information.
```

it takes forever to launch the tpu. 




-------

#### test2

```python
(base) ~ gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "cd ~/test_trainer && sudo env LD_LIBRARY_PATH=/usr/local/lib python3 test_ray_dataset_remote_jax_ttexample2.py" --worker 0
```

```python

(BaseWorkerMixin pid=47755, ip=10.128.15.201) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=54645) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(raylet, ip=10.128.15.200) 2022-06-03 17:53:06,764	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.200 --node-manager-port=39403 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=64639 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=11 --runtime-env-hash=-1022478060
(pid=46460, ip=10.128.15.200) WARNING: Logging before InitGoogle() is written to STDERR
(pid=46460, ip=10.128.15.200) I0000 00:00:1654278787.839893   46460 tpu_initializer_helper.cc:165] libtpu.so already in use by another process probably owned by another user. Run "$ sudo lsof -w /dev/accel0" to figure out which process is using the TPU. Not attempting to load libtpu.so in this process.
(pid=46460, ip=10.128.15.200) 2022-06-03 17:53:08.291155: W tensorflow/stream_executor/platform/default/dso_loader.cc:64] Could not load dynamic library 'libcudart.so.11.0'; dlerror: libcudart.so.11.0: cannot open shared object file: No such file or directory; LD_LIBRARY_PATH: /usr/local/lib
(raylet, ip=10.128.15.200) 2022-06-03 17:53:10,554	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.200 --node-manager-port=39403 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=64639 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=12 --runtime-env-hash=-1022478060
(raylet, ip=10.128.15.199) 2022-06-03 17:53:10,552	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.199 --node-manager-port=45799 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=62486 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=15 --runtime-env-hash=-1022478060
(raylet, ip=10.128.15.201) 2022-06-03 17:53:10,552	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.201 --node-manager-port=32831 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=63871 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=15 --runtime-env-hash=-1022478060
(raylet) 2022-06-03 17:53:10,555	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.198 --node-manager-port=43987 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=62164 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=51 --runtime-env-hash=-1022478060
(raylet, ip=10.128.15.199) 2022-06-03 17:53:12,041	INFO context.py:70 -- Exec'ing worker with command: exec /usr/bin/python3 /usr/local/lib/python3.8/dist-packages/ray/workers/default_worker.py --node-ip-address=10.128.15.199 --node-manager-port=45799 --object-store-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/plasma_store --raylet-name=/tmp/ray/session_2022-06-03_17-25-11_335179_36955/sockets/raylet --redis-address=None --storage=None --temp-dir=/tmp/ray --metrics-agent-port=62486 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --gcs-address=10.128.15.198:6379 --redis-password=5241590000000000 --startup-token=16 --runtime-env-hash=-1801650072
2022-06-03 17:53:31,144	INFO tune.py:752 -- Total run time: 25.36 seconds (24.82 seconds for the tuning loop).
(BaseWorkerMixin pid=46493, ip=10.128.15.200) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
(BaseWorkerMixin pid=48157, ip=10.128.15.199) [TpuDevice(id=0, process_index=0, coords=(0,0,0), core_on_chip=0), TpuDevice(id=1, process_index=0, coords=(0,0,0), core_on_chip=1), TpuDevice(id=2, process_index=0, coords=(1,0,0), core_on_chip=0), TpuDevice(id=3, process_index=0, coords=(1,0,0), core_on_chip=1), TpuDevice(id=8, process_index=0, coords=(0,1,0), core_on_chip=0), TpuDevice(id=9, process_index=0, coords=(0,1,0), core_on_chip=1), TpuDevice(id=10, process_index=0, coords=(1,1,0), core_on_chip=0), TpuDevice(id=11, process_index=0, coords=(1,1,0), core_on_chip=1), TpuDevice(id=4, process_index=1, coords=(2,0,0), core_on_chip=0), TpuDevice(id=5, process_index=1, coords=(2,0,0), core_on_chip=1), TpuDevice(id=6, process_index=1, coords=(3,0,0), core_on_chip=0), TpuDevice(id=7, process_index=1, coords=(3,0,0), core_on_chip=1), TpuDevice(id=12, process_index=1, coords=(2,1,0), core_on_chip=0), TpuDevice(id=13, process_index=1, coords=(2,1,0), core_on_chip=1), TpuDevice(id=14, process_index=1, coords=(3,1,0), core_on_chip=0), TpuDevice(id=15, process_index=1, coords=(3,1,0), core_on_chip=1), TpuDevice(id=16, process_index=2, coords=(0,2,0), core_on_chip=0), TpuDevice(id=17, process_index=2, coords=(0,2,0), core_on_chip=1), TpuDevice(id=18, process_index=2, coords=(1,2,0), core_on_chip=0), TpuDevice(id=19, process_index=2, coords=(1,2,0), core_on_chip=1), TpuDevice(id=24, process_index=2, coords=(0,3,0), core_on_chip=0), TpuDevice(id=25, process_index=2, coords=(0,3,0), core_on_chip=1), TpuDevice(id=26, process_index=2, coords=(1,3,0), core_on_chip=0), TpuDevice(id=27, process_index=2, coords=(1,3,0), core_on_chip=1), TpuDevice(id=20, process_index=3, coords=(2,2,0), core_on_chip=0), TpuDevice(id=21, process_index=3, coords=(2,2,0), core_on_chip=1), TpuDevice(id=22, process_index=3, coords=(3,2,0), core_on_chip=0), TpuDevice(id=23, process_index=3, coords=(3,2,0), core_on_chip=1), TpuDevice(id=28, process_index=3, coords=(2,3,0), core_on_chip=0), TpuDevice(id=29, process_index=3, coords=(2,3,0), core_on_chip=1), TpuDevice(id=30, process_index=3, coords=(3,3,0), core_on_chip=0), TpuDevice(id=31, process_index=3, coords=(3,3,0), core_on_chip=1)]
== Status ==
Current time: 2022-06-03 17:53:30 (running for 00:00:24.02)
Memory usage on this node: 5.9/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-53-05
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_055fd_00000 | RUNNING  | 10.128.15.200:46460 |
+------------------------+----------+---------------------+


(BaseWorkerMixin pid=54645) 5
(BaseWorkerMixin pid=54645) 5
(BaseWorkerMixin pid=54645) (5, 784)
(BaseWorkerMixin pid=47755, ip=10.128.15.201) 5
(BaseWorkerMixin pid=47755, ip=10.128.15.201) 5
(BaseWorkerMixin pid=47755, ip=10.128.15.201) (5, 784)
(BaseWorkerMixin pid=46493, ip=10.128.15.200) 5
(BaseWorkerMixin pid=46493, ip=10.128.15.200) 5
(BaseWorkerMixin pid=46493, ip=10.128.15.200) (5, 784)
(BaseWorkerMixin pid=48157, ip=10.128.15.199) 5
(BaseWorkerMixin pid=48157, ip=10.128.15.199) 5
(BaseWorkerMixin pid=48157, ip=10.128.15.199) (5, 784)
```



-------
#### test3:  training the mlp

```python
gcloud alpha compute tpus tpu-vm ssh jax-trainer-mnist-tpu-pod --zone=us-central1-a --command "cd ~/test_trainer && sudo env LD_LIBRARY_PATH=/usr/local/lib python3 test_jax_mnist_example_ray_dataset.py" --worker 0
```

```python
(BaseWorkerMixin pid=49388, ip=10.128.15.201) 4 8 32 2 4
(BaseWorkerMixin pid=56364) 4 8 32 0 4
(BaseWorkerMixin pid=48113, ip=10.128.15.200) 4 8 32 3 4
(BaseWorkerMixin pid=49825, ip=10.128.15.199) 4 8 32 1 4
== Status ==
Current time: 2022-06-03 17:59:01 (running for 00:00:28.96)
Memory usage on this node: 6.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-58-32
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_c7e1e_00000 | RUNNING  | 10.128.15.201:49355 |
+------------------------+----------+---------------------+


(BaseWorkerMixin pid=56364) /usr/local/lib/python3.8/dist-packages/jax/_src/tree_util.py:188: FutureWarning: jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() instead as a drop-in replacement.
(BaseWorkerMixin pid=56364)   warnings.warn('jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() '
(BaseWorkerMixin pid=48113, ip=10.128.15.200) /usr/local/lib/python3.8/dist-packages/jax/_src/tree_util.py:188: FutureWarning: jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() instead as a drop-in replacement.
(BaseWorkerMixin pid=48113, ip=10.128.15.200)   warnings.warn('jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() '
(BaseWorkerMixin pid=49825, ip=10.128.15.199) /usr/local/lib/python3.8/dist-packages/jax/_src/tree_util.py:188: FutureWarning: jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() instead as a drop-in replacement.
(BaseWorkerMixin pid=49825, ip=10.128.15.199)   warnings.warn('jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() '
(BaseWorkerMixin pid=49388, ip=10.128.15.201) /usr/local/lib/python3.8/dist-packages/jax/_src/tree_util.py:188: FutureWarning: jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() instead as a drop-in replacement.
(BaseWorkerMixin pid=49388, ip=10.128.15.201)   warnings.warn('jax.tree_util.tree_multimap() is deprecated. Please use jax.tree_util.tree_map() '
== Status ==
Current time: 2022-06-03 17:59:06 (running for 00:00:33.96)
Memory usage on this node: 6.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 5.0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (4.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-58-32
Number of trials: 1/1 (1 RUNNING)
+------------------------+----------+---------------------+
| Trial name             | status   | loc                 |
|------------------------+----------+---------------------|
| JaxTrainer_c7e1e_00000 | RUNNING  | 10.128.15.201:49355 |
+------------------------+----------+---------------------+


Result for JaxTrainer_c7e1e_00000:
  _time_this_iter_s: 24.568263053894043
  _timestamp: 1654279146
  _training_iteration: 1
  date: 2022-06-03_17-59-06
  done: false
  experiment_id: 279f4b15b89a4bdfa40e8b8d25dd895d
  hostname: t1v-n-e8b2b80d-w-1
  iterations_since_restore: 1
  node_ip: 10.128.15.201
  pid: 49355
  time_since_restore: 30.25447416305542
  time_this_iter_s: 30.25447416305542
  time_total_s: 30.25447416305542
  timestamp: 1654279146
  timesteps_since_restore: 0
  train_accuracy: 0.18000000715255737
  train_loss: 2.2454776763916016
  training_iteration: 1
  trial_id: c7e1e_00000
  warmup_time: 0.0040662288665771484

(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  1, train_loss: 2.2455, train_accuracy: 18.00, epoch_time: 0.767
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  2, train_loss: 1.9817, train_accuracy: 58.91, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch:  1, train_loss: 2.2455, train_accuracy: 18.00, epoch_time: 1.045
(BaseWorkerMixin pid=56364) epoch:  2, train_loss: 1.9817, train_accuracy: 58.91, epoch_time: 0.011
(BaseWorkerMixin pid=56364) epoch:  3, train_loss: 1.4962, train_accuracy: 73.79, epoch_time: 0.011
(BaseWorkerMixin pid=56364) epoch:  4, train_loss: 0.9364, train_accuracy: 79.30, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch:  5, train_loss: 0.6218, train_accuracy: 82.56, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch:  6, train_loss: 0.5105, train_accuracy: 84.75, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  1, train_loss: 2.2455, train_accuracy: 18.00, epoch_time: 0.797
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  2, train_loss: 1.9817, train_accuracy: 58.91, epoch_time: 0.011
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  3, train_loss: 1.4962, train_accuracy: 73.79, epoch_time: 0.010
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  4, train_loss: 0.9364, train_accuracy: 79.30, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  5, train_loss: 0.6218, train_accuracy: 82.56, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  6, train_loss: 0.5105, train_accuracy: 84.75, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  1, train_loss: 2.2455, train_accuracy: 18.00, epoch_time: 0.869
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  2, train_loss: 1.9817, train_accuracy: 58.91, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  3, train_loss: 1.4962, train_accuracy: 73.79, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  4, train_loss: 0.9364, train_accuracy: 79.30, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  5, train_loss: 0.6218, train_accuracy: 82.56, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  6, train_loss: 0.5105, train_accuracy: 84.75, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  7, train_loss: 0.4633, train_accuracy: 86.75, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  3, train_loss: 1.4962, train_accuracy: 73.79, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  4, train_loss: 0.9364, train_accuracy: 79.30, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  5, train_loss: 0.6218, train_accuracy: 82.56, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  6, train_loss: 0.5105, train_accuracy: 84.75, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  7, train_loss: 0.4633, train_accuracy: 86.75, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  8, train_loss: 0.4392, train_accuracy: 87.86, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch:  9, train_loss: 0.4180, train_accuracy: 88.53, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 10, train_loss: 0.3935, train_accuracy: 89.14, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 11, train_loss: 0.3727, train_accuracy: 89.67, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 12, train_loss: 0.3537, train_accuracy: 89.95, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  7, train_loss: 0.4633, train_accuracy: 86.75, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  8, train_loss: 0.4392, train_accuracy: 87.86, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch:  9, train_loss: 0.4180, train_accuracy: 88.53, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 10, train_loss: 0.3935, train_accuracy: 89.14, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 11, train_loss: 0.3727, train_accuracy: 89.67, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 12, train_loss: 0.3537, train_accuracy: 89.95, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 13, train_loss: 0.3401, train_accuracy: 90.22, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 14, train_loss: 0.3375, train_accuracy: 90.06, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 15, train_loss: 0.3445, train_accuracy: 89.72, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 16, train_loss: 0.3650, train_accuracy: 88.96, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch:  7, train_loss: 0.4633, train_accuracy: 86.75, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch:  8, train_loss: 0.4392, train_accuracy: 87.86, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch:  9, train_loss: 0.4180, train_accuracy: 88.53, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 10, train_loss: 0.3935, train_accuracy: 89.14, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 11, train_loss: 0.3727, train_accuracy: 89.67, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 12, train_loss: 0.3537, train_accuracy: 89.95, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 13, train_loss: 0.3401, train_accuracy: 90.22, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 14, train_loss: 0.3375, train_accuracy: 90.06, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 15, train_loss: 0.3445, train_accuracy: 89.72, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 16, train_loss: 0.3650, train_accuracy: 88.96, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 17, train_loss: 0.3200, train_accuracy: 90.37, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  8, train_loss: 0.4392, train_accuracy: 87.86, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch:  9, train_loss: 0.4180, train_accuracy: 88.53, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 10, train_loss: 0.3935, train_accuracy: 89.14, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 11, train_loss: 0.3727, train_accuracy: 89.67, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 12, train_loss: 0.3537, train_accuracy: 89.95, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 13, train_loss: 0.3401, train_accuracy: 90.22, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 14, train_loss: 0.3375, train_accuracy: 90.06, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 15, train_loss: 0.3445, train_accuracy: 89.72, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 16, train_loss: 0.3650, train_accuracy: 88.96, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 17, train_loss: 0.3200, train_accuracy: 90.37, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 18, train_loss: 0.2930, train_accuracy: 91.44, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 13, train_loss: 0.3401, train_accuracy: 90.22, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 14, train_loss: 0.3375, train_accuracy: 90.06, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 15, train_loss: 0.3445, train_accuracy: 89.72, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 16, train_loss: 0.3650, train_accuracy: 88.96, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 17, train_loss: 0.3200, train_accuracy: 90.37, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 18, train_loss: 0.2930, train_accuracy: 91.44, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 19, train_loss: 0.2694, train_accuracy: 92.22, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 20, train_loss: 0.2578, train_accuracy: 92.46, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 21, train_loss: 0.2475, train_accuracy: 92.86, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 22, train_loss: 0.2402, train_accuracy: 93.06, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 23, train_loss: 0.2318, train_accuracy: 93.31, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 17, train_loss: 0.3200, train_accuracy: 90.37, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 18, train_loss: 0.2930, train_accuracy: 91.44, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 19, train_loss: 0.2694, train_accuracy: 92.22, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 20, train_loss: 0.2578, train_accuracy: 92.46, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 21, train_loss: 0.2475, train_accuracy: 92.86, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 22, train_loss: 0.2402, train_accuracy: 93.06, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 23, train_loss: 0.2318, train_accuracy: 93.31, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 24, train_loss: 0.2259, train_accuracy: 93.55, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 25, train_loss: 0.2194, train_accuracy: 93.73, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 26, train_loss: 0.2145, train_accuracy: 93.91, epoch_time: 0.010
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 27, train_loss: 0.2091, train_accuracy: 94.08, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 18, train_loss: 0.2930, train_accuracy: 91.44, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 19, train_loss: 0.2694, train_accuracy: 92.22, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 20, train_loss: 0.2578, train_accuracy: 92.46, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 21, train_loss: 0.2475, train_accuracy: 92.86, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 22, train_loss: 0.2402, train_accuracy: 93.06, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 23, train_loss: 0.2318, train_accuracy: 93.31, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 24, train_loss: 0.2259, train_accuracy: 93.55, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 25, train_loss: 0.2194, train_accuracy: 93.73, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 26, train_loss: 0.2145, train_accuracy: 93.91, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 27, train_loss: 0.2091, train_accuracy: 94.08, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 28, train_loss: 0.2048, train_accuracy: 94.19, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 19, train_loss: 0.2694, train_accuracy: 92.22, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 20, train_loss: 0.2578, train_accuracy: 92.46, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 21, train_loss: 0.2475, train_accuracy: 92.86, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 22, train_loss: 0.2402, train_accuracy: 93.06, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 23, train_loss: 0.2318, train_accuracy: 93.31, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 24, train_loss: 0.2259, train_accuracy: 93.55, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 25, train_loss: 0.2194, train_accuracy: 93.73, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 26, train_loss: 0.2145, train_accuracy: 93.91, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 27, train_loss: 0.2091, train_accuracy: 94.08, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 28, train_loss: 0.2048, train_accuracy: 94.19, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 29, train_loss: 0.2000, train_accuracy: 94.35, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 24, train_loss: 0.2259, train_accuracy: 93.55, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 25, train_loss: 0.2194, train_accuracy: 93.73, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 26, train_loss: 0.2145, train_accuracy: 93.91, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 27, train_loss: 0.2091, train_accuracy: 94.08, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 28, train_loss: 0.2048, train_accuracy: 94.19, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 29, train_loss: 0.2000, train_accuracy: 94.35, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 30, train_loss: 0.1960, train_accuracy: 94.43, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 31, train_loss: 0.1918, train_accuracy: 94.55, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 32, train_loss: 0.1880, train_accuracy: 94.64, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 33, train_loss: 0.1842, train_accuracy: 94.76, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 28, train_loss: 0.2048, train_accuracy: 94.19, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 29, train_loss: 0.2000, train_accuracy: 94.35, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 30, train_loss: 0.1960, train_accuracy: 94.43, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 31, train_loss: 0.1918, train_accuracy: 94.55, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 32, train_loss: 0.1880, train_accuracy: 94.64, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 33, train_loss: 0.1842, train_accuracy: 94.76, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 34, train_loss: 0.1807, train_accuracy: 94.82, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 35, train_loss: 0.1772, train_accuracy: 94.94, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 36, train_loss: 0.1739, train_accuracy: 95.01, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 37, train_loss: 0.1706, train_accuracy: 95.09, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 38, train_loss: 0.1676, train_accuracy: 95.16, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 29, train_loss: 0.2000, train_accuracy: 94.35, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 30, train_loss: 0.1960, train_accuracy: 94.43, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 31, train_loss: 0.1918, train_accuracy: 94.55, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 32, train_loss: 0.1880, train_accuracy: 94.64, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 33, train_loss: 0.1842, train_accuracy: 94.76, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 34, train_loss: 0.1807, train_accuracy: 94.82, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 35, train_loss: 0.1772, train_accuracy: 94.94, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 36, train_loss: 0.1739, train_accuracy: 95.01, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 37, train_loss: 0.1706, train_accuracy: 95.09, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 38, train_loss: 0.1676, train_accuracy: 95.16, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 39, train_loss: 0.1645, train_accuracy: 95.26, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 30, train_loss: 0.1960, train_accuracy: 94.43, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 31, train_loss: 0.1918, train_accuracy: 94.55, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 32, train_loss: 0.1880, train_accuracy: 94.64, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 33, train_loss: 0.1842, train_accuracy: 94.76, epoch_time: 0.010
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 34, train_loss: 0.1807, train_accuracy: 94.82, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 35, train_loss: 0.1772, train_accuracy: 94.94, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 36, train_loss: 0.1739, train_accuracy: 95.01, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 37, train_loss: 0.1706, train_accuracy: 95.09, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 38, train_loss: 0.1676, train_accuracy: 95.16, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 39, train_loss: 0.1645, train_accuracy: 95.26, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 34, train_loss: 0.1807, train_accuracy: 94.82, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 35, train_loss: 0.1772, train_accuracy: 94.94, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 36, train_loss: 0.1739, train_accuracy: 95.01, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 37, train_loss: 0.1706, train_accuracy: 95.09, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 38, train_loss: 0.1676, train_accuracy: 95.16, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 39, train_loss: 0.1645, train_accuracy: 95.26, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 40, train_loss: 0.1616, train_accuracy: 95.34, epoch_time: 0.015
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 41, train_loss: 0.1587, train_accuracy: 95.43, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 42, train_loss: 0.1560, train_accuracy: 95.51, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 43, train_loss: 0.1532, train_accuracy: 95.57, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 44, train_loss: 0.1506, train_accuracy: 95.66, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 39, train_loss: 0.1645, train_accuracy: 95.26, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 40, train_loss: 0.1616, train_accuracy: 95.34, epoch_time: 0.015
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 41, train_loss: 0.1587, train_accuracy: 95.43, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 42, train_loss: 0.1560, train_accuracy: 95.51, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 43, train_loss: 0.1532, train_accuracy: 95.57, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 44, train_loss: 0.1506, train_accuracy: 95.66, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 45, train_loss: 0.1481, train_accuracy: 95.75, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 46, train_loss: 0.1456, train_accuracy: 95.83, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 47, train_loss: 0.1432, train_accuracy: 95.90, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 48, train_loss: 0.1408, train_accuracy: 95.97, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 40, train_loss: 0.1616, train_accuracy: 95.34, epoch_time: 0.015
(BaseWorkerMixin pid=56364) epoch: 41, train_loss: 0.1587, train_accuracy: 95.43, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 42, train_loss: 0.1560, train_accuracy: 95.51, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 43, train_loss: 0.1532, train_accuracy: 95.57, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 44, train_loss: 0.1506, train_accuracy: 95.66, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 45, train_loss: 0.1481, train_accuracy: 95.75, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 46, train_loss: 0.1456, train_accuracy: 95.83, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 47, train_loss: 0.1432, train_accuracy: 95.90, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 48, train_loss: 0.1408, train_accuracy: 95.97, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 49, train_loss: 0.1385, train_accuracy: 96.04, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 50, train_loss: 0.1363, train_accuracy: 96.13, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 40, train_loss: 0.1616, train_accuracy: 95.34, epoch_time: 0.015
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 41, train_loss: 0.1587, train_accuracy: 95.43, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 42, train_loss: 0.1560, train_accuracy: 95.51, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 43, train_loss: 0.1532, train_accuracy: 95.57, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 44, train_loss: 0.1506, train_accuracy: 95.66, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 45, train_loss: 0.1481, train_accuracy: 95.75, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 46, train_loss: 0.1456, train_accuracy: 95.83, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 47, train_loss: 0.1432, train_accuracy: 95.90, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 48, train_loss: 0.1408, train_accuracy: 95.97, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 49, train_loss: 0.1385, train_accuracy: 96.04, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 50, train_loss: 0.1363, train_accuracy: 96.13, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 45, train_loss: 0.1481, train_accuracy: 95.75, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 46, train_loss: 0.1456, train_accuracy: 95.83, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 47, train_loss: 0.1432, train_accuracy: 95.90, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 48, train_loss: 0.1408, train_accuracy: 95.97, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 49, train_loss: 0.1385, train_accuracy: 96.04, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 50, train_loss: 0.1363, train_accuracy: 96.13, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 51, train_loss: 0.1341, train_accuracy: 96.19, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 52, train_loss: 0.1320, train_accuracy: 96.26, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 53, train_loss: 0.1299, train_accuracy: 96.29, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 54, train_loss: 0.1279, train_accuracy: 96.34, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 55, train_loss: 0.1259, train_accuracy: 96.42, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 49, train_loss: 0.1385, train_accuracy: 96.04, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 50, train_loss: 0.1363, train_accuracy: 96.13, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 51, train_loss: 0.1341, train_accuracy: 96.19, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 52, train_loss: 0.1320, train_accuracy: 96.26, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 53, train_loss: 0.1299, train_accuracy: 96.29, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 54, train_loss: 0.1279, train_accuracy: 96.34, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 55, train_loss: 0.1259, train_accuracy: 96.42, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 56, train_loss: 0.1239, train_accuracy: 96.47, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 57, train_loss: 0.1220, train_accuracy: 96.54, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 58, train_loss: 0.1202, train_accuracy: 96.59, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 59, train_loss: 0.1184, train_accuracy: 96.63, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 51, train_loss: 0.1341, train_accuracy: 96.19, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 52, train_loss: 0.1320, train_accuracy: 96.26, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 53, train_loss: 0.1299, train_accuracy: 96.29, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 54, train_loss: 0.1279, train_accuracy: 96.34, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 55, train_loss: 0.1259, train_accuracy: 96.42, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 56, train_loss: 0.1239, train_accuracy: 96.47, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 57, train_loss: 0.1220, train_accuracy: 96.54, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 58, train_loss: 0.1202, train_accuracy: 96.59, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 59, train_loss: 0.1184, train_accuracy: 96.63, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 60, train_loss: 0.1166, train_accuracy: 96.70, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 61, train_loss: 0.1149, train_accuracy: 96.76, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 51, train_loss: 0.1341, train_accuracy: 96.19, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 52, train_loss: 0.1320, train_accuracy: 96.26, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 53, train_loss: 0.1299, train_accuracy: 96.29, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 54, train_loss: 0.1279, train_accuracy: 96.34, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 55, train_loss: 0.1259, train_accuracy: 96.42, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 56, train_loss: 0.1239, train_accuracy: 96.47, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 57, train_loss: 0.1220, train_accuracy: 96.54, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 58, train_loss: 0.1202, train_accuracy: 96.59, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 59, train_loss: 0.1184, train_accuracy: 96.63, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 60, train_loss: 0.1166, train_accuracy: 96.70, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 61, train_loss: 0.1149, train_accuracy: 96.76, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 56, train_loss: 0.1239, train_accuracy: 96.47, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 57, train_loss: 0.1220, train_accuracy: 96.54, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 58, train_loss: 0.1202, train_accuracy: 96.59, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 59, train_loss: 0.1184, train_accuracy: 96.63, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 60, train_loss: 0.1166, train_accuracy: 96.70, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 61, train_loss: 0.1149, train_accuracy: 96.76, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 62, train_loss: 0.1132, train_accuracy: 96.82, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 63, train_loss: 0.1115, train_accuracy: 96.87, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 64, train_loss: 0.1099, train_accuracy: 96.91, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 65, train_loss: 0.1083, train_accuracy: 96.95, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 60, train_loss: 0.1166, train_accuracy: 96.70, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 61, train_loss: 0.1149, train_accuracy: 96.76, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 62, train_loss: 0.1132, train_accuracy: 96.82, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 63, train_loss: 0.1115, train_accuracy: 96.87, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 64, train_loss: 0.1099, train_accuracy: 96.91, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 65, train_loss: 0.1083, train_accuracy: 96.95, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 66, train_loss: 0.1068, train_accuracy: 96.98, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 67, train_loss: 0.1053, train_accuracy: 97.00, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 68, train_loss: 0.1037, train_accuracy: 97.06, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 69, train_loss: 0.1023, train_accuracy: 97.10, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 70, train_loss: 0.1009, train_accuracy: 97.15, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 62, train_loss: 0.1132, train_accuracy: 96.82, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 63, train_loss: 0.1115, train_accuracy: 96.87, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 64, train_loss: 0.1099, train_accuracy: 96.91, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 65, train_loss: 0.1083, train_accuracy: 96.95, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 66, train_loss: 0.1068, train_accuracy: 96.98, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 67, train_loss: 0.1053, train_accuracy: 97.00, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 68, train_loss: 0.1037, train_accuracy: 97.06, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 69, train_loss: 0.1023, train_accuracy: 97.10, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 70, train_loss: 0.1009, train_accuracy: 97.15, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 71, train_loss: 0.0995, train_accuracy: 97.19, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 72, train_loss: 0.0981, train_accuracy: 97.24, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 62, train_loss: 0.1132, train_accuracy: 96.82, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 63, train_loss: 0.1115, train_accuracy: 96.87, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 64, train_loss: 0.1099, train_accuracy: 96.91, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 65, train_loss: 0.1083, train_accuracy: 96.95, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 66, train_loss: 0.1068, train_accuracy: 96.98, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 67, train_loss: 0.1053, train_accuracy: 97.00, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 68, train_loss: 0.1037, train_accuracy: 97.06, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 69, train_loss: 0.1023, train_accuracy: 97.10, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 70, train_loss: 0.1009, train_accuracy: 97.15, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 71, train_loss: 0.0995, train_accuracy: 97.19, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 72, train_loss: 0.0981, train_accuracy: 97.24, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 66, train_loss: 0.1068, train_accuracy: 96.98, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 67, train_loss: 0.1053, train_accuracy: 97.00, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 68, train_loss: 0.1037, train_accuracy: 97.06, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 69, train_loss: 0.1023, train_accuracy: 97.10, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 70, train_loss: 0.1009, train_accuracy: 97.15, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 71, train_loss: 0.0995, train_accuracy: 97.19, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 72, train_loss: 0.0981, train_accuracy: 97.24, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 73, train_loss: 0.0967, train_accuracy: 97.26, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 74, train_loss: 0.0954, train_accuracy: 97.30, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 75, train_loss: 0.0941, train_accuracy: 97.35, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 76, train_loss: 0.0928, train_accuracy: 97.39, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 71, train_loss: 0.0995, train_accuracy: 97.19, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 72, train_loss: 0.0981, train_accuracy: 97.24, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 73, train_loss: 0.0967, train_accuracy: 97.26, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 74, train_loss: 0.0954, train_accuracy: 97.30, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 75, train_loss: 0.0941, train_accuracy: 97.35, epoch_time: 0.010
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 76, train_loss: 0.0928, train_accuracy: 97.39, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 77, train_loss: 0.0916, train_accuracy: 97.42, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 78, train_loss: 0.0903, train_accuracy: 97.45, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 79, train_loss: 0.0891, train_accuracy: 97.49, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 80, train_loss: 0.0879, train_accuracy: 97.53, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 73, train_loss: 0.0967, train_accuracy: 97.26, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 74, train_loss: 0.0954, train_accuracy: 97.30, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 75, train_loss: 0.0941, train_accuracy: 97.35, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 76, train_loss: 0.0928, train_accuracy: 97.39, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 77, train_loss: 0.0916, train_accuracy: 97.42, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 78, train_loss: 0.0903, train_accuracy: 97.45, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 79, train_loss: 0.0891, train_accuracy: 97.49, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 80, train_loss: 0.0879, train_accuracy: 97.53, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 81, train_loss: 0.0868, train_accuracy: 97.57, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 82, train_loss: 0.0856, train_accuracy: 97.60, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 73, train_loss: 0.0967, train_accuracy: 97.26, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 74, train_loss: 0.0954, train_accuracy: 97.30, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 75, train_loss: 0.0941, train_accuracy: 97.35, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 76, train_loss: 0.0928, train_accuracy: 97.39, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 77, train_loss: 0.0916, train_accuracy: 97.42, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 78, train_loss: 0.0903, train_accuracy: 97.45, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 79, train_loss: 0.0891, train_accuracy: 97.49, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 80, train_loss: 0.0879, train_accuracy: 97.53, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 81, train_loss: 0.0868, train_accuracy: 97.57, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 82, train_loss: 0.0856, train_accuracy: 97.60, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 83, train_loss: 0.0845, train_accuracy: 97.63, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 77, train_loss: 0.0916, train_accuracy: 97.42, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 78, train_loss: 0.0903, train_accuracy: 97.45, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 79, train_loss: 0.0891, train_accuracy: 97.49, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 80, train_loss: 0.0879, train_accuracy: 97.53, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 81, train_loss: 0.0868, train_accuracy: 97.57, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 82, train_loss: 0.0856, train_accuracy: 97.60, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 83, train_loss: 0.0845, train_accuracy: 97.63, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 84, train_loss: 0.0834, train_accuracy: 97.66, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 85, train_loss: 0.0823, train_accuracy: 97.70, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 86, train_loss: 0.0813, train_accuracy: 97.73, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 87, train_loss: 0.0802, train_accuracy: 97.77, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 81, train_loss: 0.0868, train_accuracy: 97.57, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 82, train_loss: 0.0856, train_accuracy: 97.60, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 83, train_loss: 0.0845, train_accuracy: 97.63, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 84, train_loss: 0.0834, train_accuracy: 97.66, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 85, train_loss: 0.0823, train_accuracy: 97.70, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 86, train_loss: 0.0813, train_accuracy: 97.73, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 87, train_loss: 0.0802, train_accuracy: 97.77, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 88, train_loss: 0.0792, train_accuracy: 97.79, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 89, train_loss: 0.0782, train_accuracy: 97.82, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 90, train_loss: 0.0772, train_accuracy: 97.86, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 91, train_loss: 0.0762, train_accuracy: 97.88, epoch_time: 0.012
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 83, train_loss: 0.0845, train_accuracy: 97.63, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 84, train_loss: 0.0834, train_accuracy: 97.66, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 85, train_loss: 0.0823, train_accuracy: 97.70, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 86, train_loss: 0.0813, train_accuracy: 97.73, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 87, train_loss: 0.0802, train_accuracy: 97.77, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 88, train_loss: 0.0792, train_accuracy: 97.79, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 89, train_loss: 0.0782, train_accuracy: 97.82, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 90, train_loss: 0.0772, train_accuracy: 97.86, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 91, train_loss: 0.0762, train_accuracy: 97.88, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 92, train_loss: 0.0752, train_accuracy: 97.92, epoch_time: 0.011
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 93, train_loss: 0.0743, train_accuracy: 97.95, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 84, train_loss: 0.0834, train_accuracy: 97.66, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 85, train_loss: 0.0823, train_accuracy: 97.70, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 86, train_loss: 0.0813, train_accuracy: 97.73, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 87, train_loss: 0.0802, train_accuracy: 97.77, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 88, train_loss: 0.0792, train_accuracy: 97.79, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 89, train_loss: 0.0782, train_accuracy: 97.82, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 90, train_loss: 0.0772, train_accuracy: 97.86, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 91, train_loss: 0.0762, train_accuracy: 97.88, epoch_time: 0.010
(BaseWorkerMixin pid=56364) epoch: 92, train_loss: 0.0752, train_accuracy: 97.92, epoch_time: 0.011
(BaseWorkerMixin pid=56364) epoch: 93, train_loss: 0.0743, train_accuracy: 97.95, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 94, train_loss: 0.0734, train_accuracy: 97.98, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 88, train_loss: 0.0792, train_accuracy: 97.79, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 89, train_loss: 0.0782, train_accuracy: 97.82, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 90, train_loss: 0.0772, train_accuracy: 97.86, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 91, train_loss: 0.0762, train_accuracy: 97.88, epoch_time: 0.010
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 92, train_loss: 0.0752, train_accuracy: 97.92, epoch_time: 0.011
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 93, train_loss: 0.0743, train_accuracy: 97.95, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 94, train_loss: 0.0734, train_accuracy: 97.98, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 95, train_loss: 0.0724, train_accuracy: 98.00, epoch_time: 0.016
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 96, train_loss: 0.0715, train_accuracy: 98.03, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 97, train_loss: 0.0707, train_accuracy: 98.05, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 92, train_loss: 0.0752, train_accuracy: 97.92, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 93, train_loss: 0.0743, train_accuracy: 97.95, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 94, train_loss: 0.0734, train_accuracy: 97.98, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 95, train_loss: 0.0724, train_accuracy: 98.00, epoch_time: 0.016
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 96, train_loss: 0.0715, train_accuracy: 98.03, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 97, train_loss: 0.0707, train_accuracy: 98.05, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 98, train_loss: 0.0698, train_accuracy: 98.09, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 99, train_loss: 0.0689, train_accuracy: 98.11, epoch_time: 0.009
(BaseWorkerMixin pid=48113, ip=10.128.15.200) epoch: 100, train_loss: 0.0681, train_accuracy: 98.14, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 94, train_loss: 0.0734, train_accuracy: 97.98, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 95, train_loss: 0.0724, train_accuracy: 98.00, epoch_time: 0.016
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 96, train_loss: 0.0715, train_accuracy: 98.03, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 97, train_loss: 0.0707, train_accuracy: 98.05, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 98, train_loss: 0.0698, train_accuracy: 98.09, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 99, train_loss: 0.0689, train_accuracy: 98.11, epoch_time: 0.009
(BaseWorkerMixin pid=49825, ip=10.128.15.199) epoch: 100, train_loss: 0.0681, train_accuracy: 98.14, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 95, train_loss: 0.0724, train_accuracy: 98.00, epoch_time: 0.016
(BaseWorkerMixin pid=56364) epoch: 96, train_loss: 0.0715, train_accuracy: 98.03, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 97, train_loss: 0.0707, train_accuracy: 98.05, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 98, train_loss: 0.0698, train_accuracy: 98.09, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 99, train_loss: 0.0689, train_accuracy: 98.11, epoch_time: 0.009
(BaseWorkerMixin pid=56364) epoch: 100, train_loss: 0.0681, train_accuracy: 98.14, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 98, train_loss: 0.0698, train_accuracy: 98.09, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 99, train_loss: 0.0689, train_accuracy: 98.11, epoch_time: 0.009
(BaseWorkerMixin pid=49388, ip=10.128.15.201) epoch: 100, train_loss: 0.0681, train_accuracy: 98.14, epoch_time: 0.009
Result for JaxTrainer_c7e1e_00000:
  _time_this_iter_s: 0.009633064270019531
  _timestamp: 1654279147
  _training_iteration: 100
  date: 2022-06-03_17-59-07
  done: true
  experiment_id: 279f4b15b89a4bdfa40e8b8d25dd895d
  experiment_tag: '0'
  hostname: t1v-n-e8b2b80d-w-1
  iterations_since_restore: 100
  node_ip: 10.128.15.201
  pid: 49355
  time_since_restore: 31.246081352233887
  time_this_iter_s: 0.02072429656982422
  time_total_s: 31.246081352233887
  timestamp: 1654279147
  timesteps_since_restore: 0
  train_accuracy: 0.9814332127571106
  train_loss: 0.06809001415967941
  training_iteration: 100
  trial_id: c7e1e_00000
  warmup_time: 0.0040662288665771484

== Status ==
Current time: 2022-06-03 17:59:08 (running for 00:00:35.79)
Memory usage on this node: 6.4/334.6 GiB
Using FIFO scheduling algorithm.
Resources requested: 0/384 CPUs, 0/0 GPUs, 0.0/922.83 GiB heap, 0.0/399.49 GiB objects (0.0/4.0 TPU)
Result logdir: /root/ray_results/JaxTrainer_2022-06-03_17-58-32
Number of trials: 1/1 (1 TERMINATED)
+------------------------+------------+---------------------+--------+------------------+--------------+------------------+--------------+
| Trial name             | status     | loc                 |   iter |   total time (s) |   train_loss |   train_accuracy |   _timestamp |
|------------------------+------------+---------------------+--------+------------------+--------------+------------------+--------------|
| JaxTrainer_c7e1e_00000 | TERMINATED | 10.128.15.201:49355 |    100 |          31.2461 |      0.06809 |         0.981433 |   1654279147 |
+------------------------+------------+---------------------+--------+------------------+--------------+------------------+--------------+


2022-06-03 17:59:08,422	INFO tune.py:752 -- Total run time: 36.31 seconds (35.79 seconds for the tuning loop).

Loss results: Result(metrics={'train_loss': 0.068090014, 'train_accuracy': 0.9814332, '_timestamp': 1654279147, '_time_this_iter_s': 0.009633064270019531, '_training_iteration': 100, 'time_this_iter_s': 0.02072429656982422, 'done': True, 'timesteps_total': None, 'episodes_total': None, 'training_iteration': 100, 'trial_id': 'c7e1e_00000', 'experiment_id': '279f4b15b89a4bdfa40e8b8d25dd895d', 'date': '2022-06-03_17-59-07', 'timestamp': 1654279147, 'time_total_s': 31.246081352233887, 'pid': 49355, 'hostname': 't1v-n-e8b2b80d-w-1', 'node_ip': '10.128.15.201', 'config': {}, 'time_since_restore': 31.246081352233887, 'timesteps_since_restore': 0, 'iterations_since_restore': 100, 'warmup_time': 0.0040662288665771484, 'experiment_tag': '0'}, checkpoint=None, error=None)

```




## Trouble-shooting

issue: 
```python
  File "/usr/lib/python3.8/ctypes/__init__.py", line 373, in __init__
    self._handle = _dlopen(self._name, mode)
OSError: libmkl_intel_lp64.so.1: cannot open shared object file: No such file or directory
```

solution: 
```python
add the environment variables: `os.environ['LD_LIBRARY_PATH']='/usr/local/lib'`
```

reference: [stack-overflow](https://stackoverflow.com/questions/67257008/oserror-libmkl-intel-lp64-so-1-cannot-open-shared-object-file-no-such-file-or)
