class PolicyMappingFn:
    """Example for a callable class specifyable in yaml files as `policy_mapping_fn`.

    See for example:
    ray/rllib/tuned_examples/alpha_star/multi-agent-cartpole-alpha-star.yaml
    """

    def __call__(self, agent_id, episode, worker, **kwargs):
        return f"p{agent_id}"
