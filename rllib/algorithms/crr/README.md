
# Logbook

# updates 05/20
- [ ] Implement CRR in RLlib 
  - [ ] policy
  - [ ] d
  
- [x] Benchmark CartPole and Pendulum performance in d3rlpy with offline RL algos including CRR? 

Pendulum-v0
CRR
- env_type: random, replay
- adv_type: mean, max
- weight_type: binary, exp (try diff betas [0.1, 1, 10])
- target_update: soft, hard

Try CartPole-v0 as well ...

  - CartPole is not gonna be supported in d3rlpy since its action space is discrete
  - Pendulum Random converges (low quality data) on all settings (bin|exp, mean|max)
  - Pendulum Replay does not converge (high quality data) on any setting
  - I think the issue is on the dataset terminal flag, run experiments with correct termination flag?
  - I fixed hte dataset terminal flag issue but replay data still diverges in critic loss
  - Moving on to RLlib implementation and testing on Pendulum-random 
  - TODO: Investigate the replay dataset and others later?

# updates 05/19

- [x] CartPole discrete data from ReAgent + policy --> Get the data into the policy loss fn
ReAgent only has DiscreteCRRTrainer implementation:
https://github.com/facebookresearch/ReAgent/blob/main/reagent/training/discrete_crr_trainer.py#L23
so Cartpole should be a good comparison testbed between our implementation and theirs.

- [ ] Clone ReAgent and see if I can run some of their flows (data generation / offline policy training and evaluation??)
I could not install their dependencies on my mac. Would probably need a linux kernel w/ GPUs?
Quickly trying out my own linux machine on Pabti machines ...
Made some progress regarding the installation but data processing fails on timeline creation due to some JavaPackage error that I could not figure out.
I'm putting a pause on ReAgent and will run [d3rlpy](https://github.com/takuseno/d3rlpy) instead.

- [ ] Read ReAgent's CRR implementation to understand the details

- [ ] Walk through ReAgent's runtime with breakpoints to understand the algorithm implementation details

- [ ] Setup a ReAgent CRR baseline

- [x] Run CartPole/Pendulum-v0 baseline with d3rlpy



# updates 05/17
- [x] Inherited CQL in skin of CRR to look at train_batch

# 05/17 Developement Road map
- Inherit from CQL (for offline data reader) and read from a file (CQL examples)
- CartPole discrete data from ReAgent + policy --> Get the data into the policy loss fn
- Once we have the data in the loss-fn, modify the advantages to match CRR advantages
- Update the policy loss to match CRR loss
- Change inheritance from CQL to DQN / SAC if it needs entropy regularization?
- Figure out the evaluation on CartPole-v0?
- Draw a step by step comparison to ReAgent's implementation
- Run CRR at larger scale to verify correctness / add more settings and flexibilities
- Find a better suited dataset for testing and upscaling the experiments
