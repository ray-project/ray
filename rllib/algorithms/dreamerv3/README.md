# DreamerV3

## Overview
An RLlib-based implementation for TensorFlow/Keras of the "DreamerV3" model-based reinforcement
learning algorithm by D. Hafner et al. (Google DeepMind) 2023

The implementation is based on RLlib's new Learner- and RLModule APIs and therefore allows
for utilizing multi-GPU machines for training (see below for tips and tricks, example configs,
and command lines).

DreamerV3 trains a world model in supervised fashion using real environment
interactions. The world model's objective is to correctly predict all aspects
of the transition dynamics of the RL environment, which includes (besides predicting the
correct next observations) predicting the received rewards as well as a boolean episode
continuation flag.  
It utilizes a recurrent GRU-based architecture called "recurrent state space model" or RSSM.
Alternatingly to training this world model, it is used to generate (or "dream") ficticious
trajectories, with which two further models are trained in an RL-fashion:
The critic and actor networks.
Just like in a standard policy gradient algorithm (e.g. REINFORCE), the critic tries to
predict a correct value function (based on the world model-predicted rewards), whereas
the actor tries to come up with good actions to take for maximizing accumulated rewards
over time.
In other words, the actual RL components of the model (actor and critic) are never
trained on real environment data, but on dreamed trajectories only.

## Note on Hyperparameter Tuning for DreamerV3
DreamerV3 is an extremely versatile and stable algorithm that not only works well on
different action- and observation spaces, including discrete and continuous actions, as well
as image and vector observations, but also almost needs no hyperparameter tuning, except for
a simple "model size" setting (from "XS" to "XL") and a value for the training ratio, which
specifies how many steps to replay from the buffer for a training update vs how many
steps to take in the actual environment.


## Note on multi-GPU Training with DreamerV3
We found that when using multiple GPUs for DreamerV3 training, the following simple
adjustments should be made to the standard config.
- Multiply the original batch size (B=16) by the number of GPUs you are using.
- Multiply the number of environments you sample from in parallel by the number of GPUs you are using.
- Use a learning rate schedule for all learning rates (world model, actor, critic) with "priming".
  - In particular, the first 10k timesteps should use low rates of `0.4` times of the published rates
    (i.e. world model: `4e-5`, critic and actor: `1.2e-5`). 
  - Over the course of the next 10k timesteps, linearly increase all rates to
    n times their published values, where `n=max(4, [num GPUs])`.


## Example Configs and Command Lines
Use the config examples and templates in
[the tuned_examples folder here](https://github.com/ray-project/ray/tree/master/rllib/tuned_examples/dreamerv3)
in combination with the following scripts and command lines in order to run RLlib's DreamerV3 algorithm in your experiments:

### Atari100k
```shell
$ cd ray/rllib/tests
$ python run_regression_tests.py --dir ../tuned_examples/dreamerv3/atari_100k.py --env ALE/Pong-v5 
```

### DeepMind Control Suite (vision)
```shell
$ cd ray/rllib/tests
$ python run_regression_tests.py --dir ../tuned_examples/dreamerv3/dm_control_suite_vision.py --env DMC/cartpole/swingup 
```
Other `--env` options for the DM Control Suite would be `--env DMC/hopper/hop`, `--env DMC/walker/walk`, etc..
Note that you can also switch on WandB logging with the above script via the options
`--wandb-key=[your WandB API key] --wandb-project=[some project name] --wandb-run-name=[some run name]`


## Results
Our results on the Atari 100k and (visual) DeepMind Control Suite benchmarks match those
reported in the paper.

### Pong-v5 (100k) 1GPU vs 2GPUs vs 4GPUs
<img src="https://github.com/ray-project/ray/tree/master/doc/source/rllib/images/dreamerv3/pong_1_2_and_4gpus.svg">

### Atari 100k
<img src="https://github.com/ray-project/ray/tree/master/doc/source/rllib/images/dreamerv3/atari100k_1_vs_4gpus.svg">

### DeepMind Control Suite (vision)
<img src="https://github.com/ray-project/ray/tree/master/doc/source/rllib/images/dreamerv3/dmc_1_vs_4gpus.svg">


## References
For more algorithm details, see the original Dreamer-V3 paper:

[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

.. and the Dreamer-V2 paper:

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
