# DreamerV3
Implementation (TensorFlow/Keras) of the "DreamerV3" model-based reinforcement learning
(RL) algorithm by D. Hafner et al. 2023

DreamerV3 train a world model in supervised fashion using real environment
interactions. The world model utilizes a recurrent GRU-based architecture
("recurrent state space model" or RSSM) and uses it to predicts rewards,
episode continuation flags, as well as, observations.
With these predictions (dreams) made by the world model, both actor
and critic are trained in classic REINFORCE fashion. In other words, the
actual RL components of the model are never trained on actual environment data,
but on dreamed trajectories only.

For more algorithm details, see:

[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

.. and the "DreamerV2" paper:

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf

## Results
TODO
