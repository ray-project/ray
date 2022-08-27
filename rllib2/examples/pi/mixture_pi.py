
###################################################################
########### Mixed action space policy
# action_space = {'torques': Box(-1, 1)^6, 'gripper': Discrete(n=2)}
# a ~ MixedDistribution({
#   'torques' ~ squashedNorm(mu(s), std(s)),
#   'gripper' ~ Categorical([p1(s), p2(s)])
# })
# example usage of the mixed_dist output
# >>> actor_loss = mixed_dist.log_prob(a)
# >>> {'torques': Tensor(0.63), 'gripper': Tensor(0.63)}
###################################################################

# see how easy it is to extend the base pi class to support mixed action spaces


@dataclass
class MixturePiConfig(PiConfig):
    pi_dict: Dict[str, Pi] = field(default_factory=dict)


class MixturePi(PiBase):
    def __init__(self, config: MixturePiConfig):
        super().__init__()
        self.config = config
        self.pis = nn.ModuleDict(pi_dict)

    def forward(self, input_dict: SampleBatch, **kwargs) -> PiOutput:
        pi_outputs = {}
        for pi_key, pi in self.pis.items():
            pi_outputs[pi_key] = pi(input_dict)

        act_dist_dict = {k: v.action_dist for k, v in pi_outputs.items()}
        return PiOutput(action_dist=MixDistribution(act_dist_dict))


