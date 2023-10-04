# Torch 2.0 Compile benchmarks on RLlib 

Torch 2.0 comes with the new `torch.compile()` [API](https://pytorch.org/docs/stable/generated/torch.compile.html#torch.compile), which leverages [torch dynamo](https://pytorch.org/docs/stable/dynamo/index.html#torchdynamo-overview) under the hood to JIT-compile wrapped code. We integrate `torch.compile()` with RLlib in the context of [RLModules](https://docs.ray.io/en/latest/rllib/rllib-rlmodule.html) and Learners. 

We have integrated this feature with RLModules. You can set the backend and mode via `framework()` API on an `AlgorithmConfig` object. Alternatively, you can compile the `RLModule` directly during stand-alone usage such as inference.


# Benchmarks

We conducted a comperhensive benchmark with this feature. 

## Inference
For the benchmarking metric, we compute the inverse of the time it takes to run `forward_exploration()` of the RLModule. We have conducted this benchmark on the default implementation of PPO RLModule under different hardware settings, torch versions, dynamo backends and modes, as well as different batch sizes. Here is a high-level summary of our findings:

| Hardware | PyTorch Version | Speedup (%) | Backend + Mode           |
|----------|----------------|-------------|--------------------------|
| CPU      | 2.0.1          | 33.92       | ipex + default           |
| CPU      | 2.1.0 nightly  | x           | ipex + default           |
| T4       | 2.0.1          | 14.05       | inductor + reduce-overhead|
| T4       | 2.1.0 nightly  | 15.01       | inductor + reduce-overhead|
| V100     | 2.0.1          | 92.43       | inductor + reduce-overhead|
| V100     | 2.1.0 nightly  | 85.71       | inductor + reduce-overhead|
| A100     | 2.0.1          | x           | inductor + reduce-overhead|
| A100     | 2.1.0 nightly  | 156.66      | inductor + reduce-overhead|


For detailed benchmarks, checkout [this google doc](https://docs.google.com/spreadsheets/d/1O7_vfGRLV7JfsClXO6stTg8snxghDRRHYBrR3f47T94/edit#gid=0). Here is the benchmarking code: [./run_inference_bm.py](./run_inference_bm.py). You can run the benchmark yourself as well:

```bash
./run_all_inference_bms.sh -bs <batch_size> --backend <dynamo_backend> --mode <dynamo_mode>
```

### Some meta level comments
1. The performance improvement depends on many factors, including the neural network architecture used, the batch size during sampling, the backend, the mode, the torch version, and many other things. The best way to optimize this is to first get the non-compiled workload learning and then do a hyper-parameter tuning on torch compile parameters on different hardware.

2. For CPU inference use the recommended inference only backends: `ipex` and `onnxrt`.

3. The speedups are more significant on more modern architectures such as A100s compared to older ones like T4.

4. Torch compile is still evolving. We noticed significant differences between the 2.0.1 release and the 2.1 nightly release. Therefore, it is important to take this into account during benchmarking your own workloads.


## Exploration

In RLlib, you can now set the configuration so that the compiled module is used during sampling of an RL agent training process. By default, the rollout workers run on CPU, therefore it is recommended to use the `ipex` or `onnxrt` backend. Having said that, you can still choose to run the sampling part on GPUs as well by setting `num_gpus_per_worker` in which case other backends can be used as well.


```
config.framework(
    "torch",
    torch_compile_worker=True,
    torch_compile_worker_dynamo_backend="ipex"
    torch_compile_worker_dynamo_mode="default",
)
```

This benchmark script runs PPO algorithm with the default model architecture for Atari-Breakout game. It will run the training for `n` iterations for both compiled and non-compiled RLModules and reports the speedup. Note that negative speedup values mean a slowdown when you compile the module. 

To run the benchmark script, you need a ray cluster comprised of at least 129 CPUs (2x64 + 1) and 2 GPUs. If this is not accessible to you, you can change the number of sampling workers and batch size to make the requirements smaller.

```
python ./run_ppo_with_inference_bm.py --backend <backend> --mode <mode>
```

Here is a summary of results:

| Backend | Mode | Speedup (%) |
|---------|------|-------------|
| onnxrt | default | -72.34 |
| onnxrt | reduce-overhead | -72.72 |
| ipex | default | 11.71 |
| ipex | reduce-overhead | 11.31 |
| ipex | max-autotune | 12.88 |


As you can see, `onnxrt` does not gain any speedups in the setup we tested (in fact it slows the workload down by 70%) while the `ipex` provides ~10% speedup. If we change the model architecture, these numbers may change. So it is very important to fix the architecture first and then search for the fastest training settings. 


## Appendix

Here is a complete view of the experiments done in this benchmark.

### T4, torch=2.0.1+cu118, backend=cudagraphs, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup        |
|------------|----------------------|---------------------|----------------|
| 1          | 1416.944392          | 1070.498744         | -0.2445019368 |
| 4          | 1229.927594          | 976.0135413         | -0.2064463419 |
| 16         | 688.3866398          | 561.2428232         | -0.1846982629 |

### T4, torch=2.0.1+cu118, backend=cudagraphs, mode=reduce-overhead
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup        |
|------------|----------------------|---------------------|----------------|
| 1          | 1444.352012          | 1061.51702          | -0.2650565712 |
| 4          | 1232.765904          | 971.4321448         | -0.211989769  |
| 16         | 684.6763992          | 558.6741867         | -0.1840317742 |

### T4, torch=2.0.1+cu118, backend=cudagraphs, mode=max-autotune
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup       |
|------------|----------------------|---------------------|----------------|
| 1          | 1450.048725          | 1073.293067         | -0.259822757   |
| 4          | 1233.033495          | 970.9492169         | -0.2125524402  |
| 16         | 680.6054645          | 555.565431          | -0.1837188211  |


### T4, torch=2.0.1+cu118, backend=inductor, mode=default
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1407.150602          | 1388.549445         | -0.01321902363    |
| 4          | 1125.619106          | 1115.115648         | -0.009331271877   |
| 16         | 685.8785968          | 683.0003853         | -0.00419638632    |


### T4, torch=2.0.1+cu118, backend=inductor, mode=reduce-overhead
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1436.253369          | 1638.012412         | 0.1404759401      |
| 4          | 1220.94159           | 1418.971087         | 0.1621940798      |
| 16         | 681.6297995          | 693.904727          | 0.01800820267     |


### T4, torch=2.0.1+cu118, backend=inductor, mode=reduce-overhead
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1436.253369          | 1638.012412         | 0.1404759401      |
| 4          | 1220.94159           | 1418.971087         | 0.1621940798      |
| 16         | 681.6297995          | 693.904727          | 0.01800820267     |

### T4, torch=2.0.1+cu118, backend=inductor, mode=max-autotune
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1387.162631          | 1610.492709         | 0.1609977612      |
| 4          | 1229.153562          | 1444.786043         | 0.1754316859      |
| 16         | 686.7980746          | 705.0833599         | 0.02662396128     |



### T4, torch=2.1.0.dev20230614+cu118, backend=cudagraphs, mode=default
---

Similar to torch 2.0 numbers.

### T4, torch=2.1.0.dev20230614+cu118, mode=reduce-overhead
---

Similar to torch 2.0 numbers.

### T4, torch=2.1.0.dev20230614+cu118,  backend=cudagraphs, mode=max-autotune
---

Similar to torch 2.0 numbers.


### T4, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=default
---

Similar to torch 2.0 numbers.


### T4, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=reduce-overhead
---

Similar to torch 2.0 numbers.

### T4, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=reduce-overhead
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1415.243828          | 1627.604089         | 0.1500520668      |
| 4          | 1205.818761          | 1363.914143         | 0.1311104022      |
| 16         | 664.9148261          | 684.8039204         | 0.02991224362     |


### T4, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=max-autotune
---

Similar to torch 2.0 numbers.



### V100, torch=2.0.1+cu118, backend=cudagraphs, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 940.8116683          | 1455.383784         | 0.5469448698      |
| 4          | 929.174622           | 1176.58136          | 0.2662650618      |
| 16         | 893.4697611          | 1307.312612         | 0.4631861861      |


### V100, torch=2.0.1+cu118, backend=cudagraphs, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 902.5531645          | 1411.217542         | 0.5635838391      |
| 4          | 946.2815455          | 1166.741298         | 0.2329747987      |
| 16         | 876.6270322          | 1316.121961         | 0.5013476799      |


### V100, torch=2.0.1+cu118, backend=cudagraphs, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 899.2288182          | 1423.560527         | 0.5830904196      |
| 4          | 934.5095328          | 1176.58136          | 0.2590362312      |
| 16         | 888.5919127          | 1314.350585         | 0.4791385862      |



### V100, torch=2.0.1+cu118, backend=inductor, mode=default
---

| Batch Size | No Compile Throughput | Compiled Throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1407.150602          | 1388.549445         | -0.01321902363    |
| 4          | 1125.619106          | 1115.115648         | -0.009331271877   |
| 16         | 685.8785968          | 683.0003853         | -0.00419638632    |


### V100, torch=2.0.1+cu118, backend=inductor, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 912.675235           | 929.174622          | 0.01807804833     |
| 4          | 949.0402977          | 949.0402977         | 0                 |
| 16         | 870.3765247          | 878.2037115         | 0.008992874482    |



### V100, torch=2.0.1+cu118, backend=inductor, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 914.3843777          | 1759.572145         | 0.9243243742      |
| 4          | 905.0625994          | 1352.579591         | 0.4944597112      |
| 16         | 898.4015545          | 1600.922162         | 0.7819672663      |


### V100, torch=2.0.1+cu118, backend=inductor, mode=max-autotune
---

Skipped.

### V100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 930.059557           | 1434.012471         | 0.541850154       |
| 4          | 930.9461792          | 1166.741298         | 0.2532854468      |
| 16         | 855.1335642          | 1295.175766         | 0.5145888551      |


### V100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=reduce-overhead
---

Skipped.

### V100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=max-autotune
---

Skipped.

### V100, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 907.5859293          | 961.1835129         | 0.05905510638     |
| 4          | 923.9001753          | 984.4380173         | 0.06552422395     |
| 16         | 885.3694334          | 931.8344934         | 0.05248098503     |


### V100, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 927.4097944          | 1722.332421         | 0.8571427985      |
| 4          | 917.8219393          | 1323.255468         | 0.4417344054      |
| 16         | 872.7099915          | 1535.475601         | 0.7594339653      |


### V100, torch=2.1.0.dev20230614+cu118, backend=inductor, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 930.9461792          | 1737.655781         | 0.8665480556      |
| 4          | 928.2913694          | 1319.679074         | 0.4216216135      |
| 16         | 864.2146097          | 1535.475601         | 0.7767295111      |


### V100, torch=2.1.0.dev20230614+cu118, backend=inductor, mode=max-autotune
---

Skipped.


### A100, torch=2.0.1+cu118, backend=cudagraphs, mode=default
---


| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1422.524449          | 2244.971293         | 0.5781600764      |
| 4          | 1451.058688          | 2239.822237         | 0.5435779795      |
| 16         | 1211.615967          | 1756.407423         | 0.449640374       |



### A100, torch=2.0.1+cu118, backend=cudagraphs, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1208.616982          | 1896.237785         | 0.5689319392      |
| 4          | 1500.095985          | 2271.075658         | 0.5139535612      |
| 16         | 1409.181137          | 1980.856883         | 0.4056793908      |


### A100, torch=2.0.1+cu118, backend=cudagraphs, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1375.440154          | 2199.465066         | 0.599099066       |
| 4          | 1401.093953          | 2194.522411         | 0.5662921145      |
| 16         | 1421.488371          | 1992.98467          | 0.4020407843      |



### A100, torch=2.0.1+cu118, backend=inductor, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1222.230956          | 1244.028691         | 0.01783438281     |
| 4          |                      |                     |                   |
| 16         | 1413.259722          | 1383.232962         | -0.02124645519    |



### A100, torch=2.0.1+cu118, backend=inductor, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 912.675235           | 929.174622          | 0.01807804833     |
| 4          | 949.0402977          | 949.0402977         | 0                 |
| 16         | 870.3765247          | 878.2037115         | 0.008992874482    |



### A100, torch=2.0.1+cu118, backend=inductor, mode=max-autotune
---

Failed due to "got an unexpected keyword argument 'mode'" error


### A100, torch=2.0.1+cu118, backend=inductor, mode=max-autotune
---

Failed due to "got an unexpected keyword argument 'mode'" error


### A100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1373.505625          | 2281.688087         | 0.6612149571      |
| 4          | 1415.30794           | 2319.625867         | 0.6389548891      |
| 16         | 1352.579591          | 1992.98467          | 0.4734694236      |


### A100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=reduce-overhead
---

Skipped.

### A100, torch=2.1.0.dev20230614+cu118,   backend=cudagraphs, mode=max-autotune
---

Skipped.

### A100, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1377.380141          | 1434.012471         | 0.04111597675     |
| 4          | 1337.756796          | 1391.114692         | 0.0398860954      |
| 16         | 1170.938241          | 1256.837228         | 0.07335910951     |



### A100, torch=2.1.0.dev20230614+cu118,  backend=inductor, mode=reduce-overhead
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1377.380141          | 1434.012471         | 0.04111597675     |
| 4          | 1337.756796          | 1391.114692         | 0.0398860954      |
| 16         | 1170.938241          | 1256.837228         | 0.07335910951     |



### A100, torch=2.1.0.dev20230614+cu118, backend=inductor, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1178.000566          | 3023.413447         | 1.566563662       |
| 4          | 1210.11466           | 3110.071612         | 1.570063578       |
| 16         | 1186.588728          | 2353.16272          | 0.9831325414      |



### A100, torch=2.1.0.dev20230614+cu118, backend=inductor, mode=max-autotune
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 1379.325608          | 3180.985245         | 1.306188782       |
| 4          | 1417.362103          | 3244.393766         | 1.289036625       |
| 16         | 1290.04293           | 2466.066991         | 0.911616221       |




### CPU, torch=2.0.1+cu118, backend=cudagraphs, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 578.1523201          | 531.928471          | -0.07995098789    |
| 4          | 331.9012918          | 311.6321031         | -0.06106992993    |
| 16         | 164.9463843          | 160.25641           | -0.0284333259     |


### CPU, torch=2.0.1+cu118, backend=cudagraphs, mode="reduce-overhead"
---

Did not run the benchmark because it did not seem promising based on default results

### CPU, torch=2.0.1+cu118, backend=cudagraphs, mode=max-autotune
---

Did not run the benchmark because it did not seem promising based on default results

### CPU, torch=2.0.1+cu118, backend=inductor, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 546.7588311          | 565.3345955         | 0.03397432888     |
| 4          | 331.510474           | 340.0472254         | 0.02575107572     |
| 16         | 164.2230245          | 168.3524304         | 0.02514510914     |


### CPU, torch=2.0.1+cu118, backend=inductor, mode="reduce-overhead"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 540.1341171          | 563.8606004         | 0.043927022       |
| 4          | 328.3528669          | 337.565962          | 0.02805851913     |
| 16         | 164.5715592          | 168.7219295         | 0.02521924417     |


### CPU, torch=2.0.1+cu118, backend=inductor, mode="max-autotune"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 580.6391773          | 598.2063383         | 0.03025486687     |
| 4          | 328.9560153          | 338.3792342         | 0.02864583246     |
| 16         | 164.5278098          | 168.3619589         | 0.02330395751     |


### CPU, torch=2.0.1+cu118, backend=onnxrt, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 567.1300498          | 763.7599065         | 0.3467103477      |
| 4          | 330.0556529          | 421.0370352         | 0.2756546707      |
| 16         | 164.5542296          | 151.9668204         | -0.07649398753    |


### CPU, torch=2.0.1+cu118, backend=onnxrt, mode="reduce-overhead"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 578.993195           | 775.3957802         | 0.3392139786      |
| 4          | 333.5218966          | 422.1717724         | 0.2657992675      |
| 16         | 160.4411238          | 151.6401364         | -0.05485493474    |


### CPU, torch=2.0.1+cu118, backend=onnxrt, mode="max-autotune"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 536.158516           | 770.1312457         | 0.4363872301      |
| 4          | 332.4220525          | 418.4212516         | 0.2587048557      |
| 16         | 163.9339995          | 112.1524862         | -0.3158680534     |


### CPU, torch=2.0.1+cu118, backend=ipex, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 584.9814324          | 796.2696504         | 0.361187905       |
| 4          | 332.1605868          | 408.9404238         | 0.2311527618      |
| 16         | 160.8793055          | 185.0103189         | 0.1499945152      |


### CPU, torch=2.0.1+cu118, backend=ipex, mode="reduce-overhead"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 584.9814324          | 799.9283288         | 0.3674422547      |
| 4          | 331.7075254          | 408.6623514         | 0.2319960208      |
| 16         | 164.4615619          | 185.7526198         | 0.123758031       |


### CPU, torch=2.0.1+cu118, backend=ipex, mode="max-autotune"
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 577.9651795          | 796.2899452         | 0.3777472648      |
| 4          | 329.1032688          | 408.9511096         | 0.2426224483      |
| 16         | 165.0165032          | 185.7526198         | 0.1256608654      |


### CPU, torch=2.1.0.dev20230614+cu118, backend=cudagraphs, mode=default
---

The numbers did not show significant speedup

### CPU, torch=2.1.0.dev20230614+cu118, backend=cudagraphs, mode="reduce-overhead"
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=cudagraphs, mode=max-autotune
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=inductor, mode=default
---

The numbers did not show significant speedup

### CPU, torch=2.1.0.dev20230614+cu118, backend=inductor, mode="reduce-overhead"
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=inductor, mode="max-autotune"
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=onnxrt, mode=default
---

| batch_size | no compile throughput | compiled throughput | Speedup          |
|------------|----------------------|---------------------|-------------------|
| 1          | 541.1677144          | 492.941076          | -0.08911588254    |
| 4          | 308.4683275          | 405.3072657         | 0.3139347854      |
| 16         | 162.7905163          | 150.3097612         | -0.07666758126    |


### CPU, torch=2.1.0.dev20230614+cu118, backend=onnxrt, mode="reduce-overhead"
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=onnxrt, mode="max-autotune"
---

Failed due to "got an unexpected keyword argument `mode` error

### CPU, torch=2.1.0.dev20230614+cu118, backend=ipex, mode=default
---
Failed due to `intel_extension_for_pytorch` not being compatible with the nightly version of torch

### CPU, torch=2.1.0.dev20230614+cu118, backend=ipex, mode="reduce-overhead"
---

Failed due to "got an unexpected keyword argument `mode` error


### CPU, torch=2.1.0.dev20230614+cu118, backend=ipex, mode="max-autotune"
---

Failed due to "got an unexpected keyword argument `mode` error
