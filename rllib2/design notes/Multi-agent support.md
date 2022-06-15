Multi-Agent support means the following scenarios:

## Independent Algorithms for updating each Pi
There can be independent Algorithms for updating each RLModule (Pi).
There is no agent communication during training.
During sampling the specific training portions of the algorithms will be ignored but
the other RLModules will be queried to get the samples for training the RLModule
of interest. See multi_agent_two_trainers.py.

```python
# This function will be used during sampling to group samples by pid
policy_map_fn=lambda agent_id: 'ppo_policy' if agent_id == 0 else 'dqn_policy'

policies = {'ppo_policy': PPORLModule, 'dqn_policy': DQNRLModule}
# Both PPORLModule and DQNRLModule have the same forward() method so in principle they
# look identical during sampling an env

ppo = PPO(..., multi_agent={policy_map_fn, policies, policies_to_train=['ppo_policy']})
dqn = DQN(..., multi_agent={policy_map_fn, policies, policies_to_train=['dqn_policy']})


for iter in range(NUM_ITER):

    # runs forward() of both ppo and dqn modules for sample collection,
    # postprocesses according to PPO requirements,
    # and then updates only the ppo_policy according to ppo update rule
    ppo.train()

    # runs forward() of both ppo and dqn modules for sample collection,
    # postprocesses according to DQN requirements,
    # and then updates only the dqn_policy according to dqn update rule
    dqn.train()

    # update the ppo_policy weights of the dqn trainer with the update ppo_policy and vice versa
    ppo.update_weight('dqn_policy', dqn.get_weight('dqn_policy'))
    dqn.update_weight('ppo_policy', ppo.get_weight('ppo_policy'))
```

###Action Items

- [x] Algorithm should create a dictionary of UnitTrainers:

    ```python
     self.unit_trainer_map = {'ppo_policy': PPOTrainer(PPORLModule), 'dqn_policy': PPOTrainer(DQNRLModule)}
    ```

- [x] Allow users to construct arbitrary RLModules inside the `.make_model` of a unit_trainer.
But if there is a mismatch between the type of the output of `forward_train()`
and the expected output type from the UnitTrainer we should raise an error to inform
them about the mismatch.

- [x] Inside `algorithm.update()` loop through the unit_trainer_map and only call `.update()`
on those policies that are included in policies_to_train

###Notes

1. In this scenario agent_k which is based on policy_m can still use data from the
perspective of other agents to encode its own observation into a latent state.
This is true for both forward() and forward_train() methods.

2. Can some part of the RLModules still be shared? 
   1. They use a shared frozen ResNet model for encoding input images?
       This is a valid use-case. See below. 
   2. They need to both train a share image encoder with two different algorithms?
       Very hard case. See below.


### Sharing modules between different RLModules before construction

Config Schema:
```python
config = dict(
    algorithm='PPO',
    ...
    multi_agent={
        'policy_mapping_fn': lambda agent_id: 'ppo_policy' if agent_id==0 else 'dqn_policy',
        'policies': {
            'ppo_policy': (PPORLModule, ppo_config),
            'dqn_policy': (DQMRLModule, dqn_config),
        },
        'policies_to_train': {'ppo_policy'}
        'shared_modules': {
            'encoder': {
                'class': Encoder,
                'config': encoder_config,
                'shared_between': {'ppo_policy': 'encoder', 'dqn_policy': 'embedder'} # the renaming that needs to happen inside the RLModules
            },
            'dynamics': {
                'class': DynamicsModel,
                'config': dynamic_config,
                'shared_between': {'ppo_policy': 'dynamics_mdl', 'dqn_policy': 'dynamics'}
            }
    }
)
```

The algorithm will essentially create the RLModules and then pass them to the corresponding UnitTrainer.
Each algorithm is built with a special UnitTrainer

```python

def setup():
    ...
    encoder = ...
    dynamics = ...
    ppo_rl_module = PPORLModule(ppo_config, encoder=encoder, dynamics_mdl=dynamics)
    dqn_rl_module = DQNRLModule(dqn_config, embedder=encoder, dynamics=dynamics)
    self.unit_trainer_map = {
        'ppo_policy': PPOUnitTrainer(ppo_rl_module, ppo_trainer_config),
        'dqn_policy': PPOUnitTrainer(dqn_rl_module, ppo_trainer_config)
    }

def training_step():
    # when and what
    train_batch: Dict[PolicyID, SampleBatch] = sample()
    results = self.update(train_batch)

def update():
    results = {}
    for pid in self.unit_trainer_map:
        results[pid] = self.unit_trainer_map[pid].update(train_batch[pid])
    return results
```

### Changes required to the design:
1. The UnitTrainer should accept an RLModule that conforms to a specific RLModuleInterface (e.g. PPORLModule)
2. The entity creating UnitTrainer should also create the rl_module object that it needs.

## A homogeneous MARL or a single Agent RL 
This is simply one policy for m agents in the env. 
This is easy to support with the design above.
```python
config = {
    algorithm : 'PPO'
    multi_agent: {
        'policies': {
            'default_policy': (PPORLModule, ppo_config),
        },
        'shared_between': None
    }
}

def setup():
    ...
    ppo_rl_module = PPORLModule(ppo_config)
    ppo_trainer = PPOUnitTrainer(ppo_rl_module, ppo_trainer_config)
    self.unit_trainer_map = {'deafult_policy': ppo_trainer}
```

## Centralized Critic MARL
Several policies controlled by one algorithm that share an encoder and a critic.

```python
config = {
    algorithm: 'PPO',
    multi_agent: {
        'policies': {
            'A': (PPORLModule, ppo_config_A),
            'B': (PPORLModule, ppo_config_B)
        }
        'shared_between': {
            'encoder': {
                'class': Encoder,
                'config': encoder_config,
                'shared_between': {'A': 'encoder', 'B': 'encoder'}
            },
            'vf': {
                'class': CentralizedVF,
                'config': vf_conf,
                'shared_between': {'A': 'vf', 'B': 'vf'}
            }
        }
    }
}

def seutp():
    ...
    encoder = ...
    vf = ...
    ppo_rl_module_A = PPORLModule(ppo_config_A, encoder=encoder, vf=vf)
    ppo_rl_module_B = PPORLModule(ppo_config_B, encoder=encoder, vf=vf)
    self.unit_trainer_map = {
        'A': PPOUnitTrainer(ppo_rl_module_A),
        'B': PPOUnitTrainer(ppo_rl_module_B)
    }
```

The centralized value function will use the observation of the current agent +
other agents's states at the same time-step (which will be provided by the custom
view_requirement) to compute vf values used in PPO.