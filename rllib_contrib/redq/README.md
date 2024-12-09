# RED Q

[Randomized Ensembled Double Q-Learning: Learning Fast Without a Model](https://arxiv.org/abs/2101.05982)  Randomized Ensembled Double Q-Learning (REDQ) extends Soft-Actor Critic (SAC) to ensemble learning.

The algorithm for for the continuous part on [1] and the discrete part is based on [2].

## Paper abstract
Using a high Update-To-Data (UTD) ratio, model-based methods have recently achieved much higher sample efficiency than previous model-free methods for continuous-action DRL benchmarks. In this paper, we introduce a simple model-free algorithm, Randomized Ensembled Double Q-Learning (REDQ), and show that its performance is just as good as, if not better than, a state-of-the-art model-based algorithm for the MuJoCo benchmark. Moreover, REDQ can achieve this performance using fewer parameters than the model-based method, and with less wall-clock run time. REDQ has three carefully integrated ingredients which allow it to achieve its high performance:

* a UTD ratio >> 1;
* an ensemble of Q functions;
* in-target minimization across a random subset of Q functions from the ensemble.

Through carefully designed experiments, we provide a detailed analysis of REDQ and related model-free algorithms. To our knowledge, REDQ is the first successful model-free DRL algorithm for continuous-action spaces using a UTD ratio >> 1.

[1] Haarnoja T, Zhou A, Hartikainen K, Tucker G, Ha S, Tan J, Kumar V, Zhu H, Gupta A, Abbeel P, Levine S. Soft actor-critic algorithms and applications. arXiv preprint arXiv:1812.05905. 2018 Dec 13.
[2] Christodoulou, Petros. "Soft actor-critic for discrete action settings." arXiv preprint arXiv:1910.07207 (2019).
[3] Chen, Xinyue, Che Wang, Zijian Zhou, and Keith Ross. "Randomized ensembled double q-learning: Learning fast without a model." arXiv preprint arXiv:2101.05982 (2021).

## Installation

```
conda create -n rllib-redq python=3.9
conda activate rllib-redq
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage


For the discrete REDQ implementation use [CartPole-v0 Example](examples/run_cartpole.py) and run

```bash
cd examples
python run_cartpole.py
```

For the continuous REDQ implementation use [Pendulum-v1 Example](examples/run_pendulum.py) and run

```bash
cd examples
python run_pendulum.py
```
