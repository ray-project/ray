


SAC(Algorithm):


    def __init__(self, config: TrainerConfigDict, env: Union[EnvType, str]):

        # say someone passes some me module ids ... they want MultiAgentSAC

        SACModule.make_MultiAgent(config["module_ids"])

    
    def default_module_class():
        return SACModule, SACOptimizer