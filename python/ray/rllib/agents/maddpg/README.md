# Implementation of Multi-Agent DDPG in RLlib

## Notes
- The code in [OpenAI/MADDPG](https://github.com/openai/maddpg) is refactored in RLlib, and test results are given in `./plots`.
    - It was tested on 7 scenarios of [OpenAI/Multi-Agent Particle Environment (MPE)](https://github.com/openai/multiagent-particle-envs).
        - `simple`, `simple_adversary`, `simple_crypto`, `simple_push`, `simple_speaker_listener`, `simple_spread`, `simple_tag`
            - RLlib MADDPG shows the similar performance as OpenAI MADDPG on 7 scenarios except `simple_crypto`. 
    - Hyperparameters were set to follow the original hyperparameter setting in [OpenAI/MADDPG](https://github.com/openai/maddpg).
    
- Empirically, it was shown that running without `lz4` shows much faster performance.

## References
- [OpenAI/MADDPG](https://github.com/openai/maddpg)
- [OpenAI/Multi-Agent Particle Environment](https://github.com/openai/multiagent-particle-envs)
    - [wsjeon/Multi-Agent Particle Environment](https://github.com/wsjeon/multiagent-particle-envs)
        - It includes the minor change for MPE to work with recent OpenAI Gym.

 
