# DreamerV3

## Overview
An RLlib-based implementation for TensorFlow and Keras of the "DreamerV3" model-based reinforcement
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
trained on actual environment data, but on such dreamed trajectories only.

## Note on Hyperparameter Tuning for DreamerV3
DreamerV3 is an extremely versatile and stable algorithm that not only works well on
different action- and observation spaces, including discrete and continuous actions, as well
as image and vector observations, but also almost needs to hyperparameter tuning, except for
a simple model-size setting (from "XS" to "XL") and a value for the training ratio (how many
steps to replay from the buffer for a training update vs how many steps to take in the
actual environment).

## Note on multi-GPU Training with DreamerV3
We found that when using multiple GPUs for DreamerV3 training, the following simple
adjustments should be made to the standard config.

## Example Configs and Command Lines
Use the config examples and templates in
[the tuned_examples folder here](https://github.com/ray-project/ray/tree/master/rllib/tuned_examples/dreamerv3)
in combination with the in order to run RLlib's DreamerV3 algorithm in your experiments.



## Results
Our results on the Atari 100k and (visual) DeepMind Control Suite benchmarks match those
reported in the paper.


## References
For more algorithm details, see:

[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

.. and the "DreamerV2" paper:

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
