import logging 

import ray
from ray.rllib.agents import with_common_config
from ray.rllib.agents.maml.maml_tf_policy import MAMLTFPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.optimizers.maml_optimizer import MAMLOptimizer
from gym.envs.registration import registry, register, make, spec

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.0005,
    # Size of batches collected from each worker
    "rollout_fragment_length": 200,
    # Stepsize of SGD
    "lr": 1e-3,
    # Share layers for value function
    "vf_share_layers": False,
    # Coefficient of the value function loss
    "vf_loss_coeff": 1.0,
    # Coefficient of the entropy regularizer
    "entropy_coeff": 0.0,
    # PPO clip parameter
    "clip_param": 0.3,
    # Clip param for the value function. Note that this is sensitive to the
    # scale of the rewards. If your expected V is large, increase this.
    "vf_clip_param": 10.0,
    # If specified, clip the global norm of gradients by this amount
    "grad_clip": None,
    # Target value for KL divergence
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Number of Inner adaptation steps for Workers
    "inner_adaptation_steps": 1,
    # Number of ProMP steps per meta-update iteration (PPO steps)
    "maml_optimizer_steps": 5,
    # Inner Adaptation Step size
    "inner_lr": 0.1,
    # Use PPO KL Loss
    "use_kl_loss": False,
    # Grad Clipping
    "grad_clip": None
})
# __sphinx_doc_end__
# yapf: enable

# @mluo: TODO
def execution_plan(workers, config):
    # Sync workers with meta policy
    workers.sync_weights()

    # Set random task for each worker
    env_configs = workers.local_worker().sample_tasks(len(workers.remote_workers()))
    ray.get([e.set_task.remote(env_configs[i]) for i,e in enumerate(workers.remote_workers())])

    # Data Collection
    meta_split = []
    samples = ray.get([e.sample.remote("0") for i,e in enumerate(workers.remote_workers())])
    meta_samples = SampleBatch.concat_samples(samples)
    meta_split.append([sample['obs'].shape[0] for sample in samples])
    for step in range(config["inner_adaptation_steps"]):
        for i,e in enumerate(workers.remote_workers()):
            e.learn_on_batch.remote(samples[i])

        samples = ray_get_and_free([e.sample.remote(str(step+1)) for e in workers.remote_workers()])
        meta_samples = meta_samples.concat(SampleBatch.concat_samples(samples))
        meta_split.append([sample['obs'].shape[0] for sample in samples])

    # Meta-update Step
    meta_samples["split"] = np.array(meta_split)
    for i in range(config["maml_optimizer_steps"]):
        fetches = self.workers.local_worker().learn_on_batch(all_samples)

    return get_learner_stats(fetches)


def after_optimizer_step(trainer, fetches):
    if trainer.config["use_kl_loss"]:
        trainer.workers.local_worker().for_policy(lambda pi: pi.update_kls(fetches["default_policy"]["inner_kl"]))


def maml_metrics(trainer):
    res = trainer.optimizer.collect_metrics(
            trainer.config["collect_metrics_timeout"],
            min_history=trainer.config["metrics_smoothing_episodes"],
            selected_workers=trainer.workers.remote_workers(),
            dataset_id=str(0))
    
    for i in range(1,trainer.config["inner_adaptation_steps"]+1):    
        res_adapt = trainer.optimizer.collect_metrics(
                trainer.config["collect_metrics_timeout"],
                min_history=trainer.config["metrics_smoothing_episodes"],
                selected_workers=trainer.workers.remote_workers(),
                dataset_id=str(i))
        res["episode_reward_max_adapt_" + str(i)] = res_adapt["episode_reward_max"]
        res["episode_reward_mean_adapt_" + str(i)] = res_adapt["episode_reward_mean"]
        res["episode_reward_min_adapt_" + str(i)] = res_adapt["episode_reward_min"]

    res["adaptation_delta"] = res["episode_reward_mean_adapt_" + str(trainer.config["inner_adaptation_steps"])] - res["episode_reward_mean"]
    return res

"""
# Fill Tensorboard with Pre/Post update Stats
def update_pre_post_stats(self, pre_res, post_res):
    pre_reward_max = pre_res['episode_reward_max']
    pre_reward_mean = pre_res['episode_reward_mean']
    pre_reward_min = pre_res['episode_reward_min']

    pre_res['episode_reward_max(post)'] = post_res['episode_reward_max']
    pre_res['episode_reward_mean(post)'] = post_res['episode_reward_mean']
    pre_res['episode_reward_min(post)'] = post_res['episode_reward_min']

    pre_res['pre-post-delta']= post_res['episode_reward_mean'] - pre_res['episode_reward_mean']

    return pre_res


@override(Trainer)
def _train(self):
    prev_steps = self.optimizer.num_steps_sampled
    fetches = self.optimizer.step()
    
    if "kl" in fetches and self.config["use_kl_loss"]:
        # single-agent
        self.workers.local_worker().for_policy(
            lambda pi: pi.update_kls(fetches["kl"]))

    # Pre adaptation metrics
    res = self.optimizer.collect_metrics("pre",
        self.config["collect_metrics_timeout"],
        min_history=self.config["metrics_smoothing_episodes"],
        selected_workers=self.workers.remote_workers())
    res.update(
        timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
        info=res.get("info", {}))
    print("Pre adaption stats", res)

    # Post adaptation metrics
    res1 = self.optimizer.collect_metrics("post",
        self.config["collect_metrics_timeout"],
        min_history=self.config["metrics_smoothing_episodes"],
        selected_workers=self.workers.remote_workers())
    print("Post adaptation stats", res1)

    res = self.update_pre_post_stats(res, res1)
    return res
"""

def get_policy_class(config):
    # @mluo: TODO
    assert config["framework"] != "torch"
    return MAMLTFPolicy

def validate_config(config):
    if config["inner_adaptation_steps"]<=0:
        raise ValueError(
            "Inner Adaptation Steps must be >=1."
            )
    if config["entropy_coeff"] < 0:
        raise DeprecationWarning("entropy_coeff must be >= 0")
    if (config["batch_mode"] == "truncate_episodes" and not config["use_gae"]):
        raise ValueError(
            "Episode truncation is not supported without a value "
            "function. Consider setting batch_mode=complete_episodes.")
    if (config["multiagent"]["policies"] and not config["simple_optimizer"]):
        logger.info(
            "In multi-agent mode, policies will be optimized sequentially "
            "by the multi-GPU optimizer. Consider setting "
            "simple_optimizer=True if this doesn't work for you.")

register(
    id='HalfCheetahRandDirec-v2',
    entry_point='ray.rllib.agents.maml.halfcheetah_rand_direc:NormalizedEnv',
    max_episode_steps=1000,
)

MAMLTrainer = build_trainer(
    name="MAML",
    default_config=DEFAULT_CONFIG,
    default_policy=MAMLTFPolicy,
    get_policy_class=get_policy_class,
    #execution_plan=execution_plan,
    make_policy_optimizer=MAMLOptimizer,
    after_optimizer_step=after_optimizer_step,
    collect_metrics_fn=maml_metrics,
    validate_config=validate_config)