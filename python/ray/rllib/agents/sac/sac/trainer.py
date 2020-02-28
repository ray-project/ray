import logging

from ray.rllib.agents.sac.sac.config import DEFAULT_CONFIG
from ray.rllib.agents.sac.sac.dev_utils import using_ray_8
from ray.rllib.agents.sac.sac.sac_policy import SACTFPolicy
from ray.rllib.agents.sac.sac.rllib_proxy._trainer_template import build_trainer
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.optimizers import SyncReplayOptimizer
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule


logger = logging.getLogger(__name__)


def make_optimizer(workers, config):
    if using_ray_8():
        return SyncReplayOptimizer(
            workers,
            learning_starts=config["learning_starts"],
            buffer_size=config["buffer_size"],
            prioritized_replay=config["prioritized_replay"],
            prioritized_replay_alpha=config["prioritized_replay_alpha"],
            prioritized_replay_beta=config["prioritized_replay_beta"],
            schedule_max_timesteps=config["schedule_max_timesteps"],
            beta_annealing_fraction=config["beta_annealing_fraction"],
            final_prioritized_replay_beta=config["final_prioritized_replay_beta"],
            prioritized_replay_eps=config["prioritized_replay_eps"],
            train_batch_size=config["train_batch_size"],
            sample_batch_size=config["sample_batch_size"],
            **config["optimizer"])
    else:
        print(f"DEBUG: sac/trainer::31 {workers}")
        local_evaluator, remote_evaluators = workers
        return SyncReplayOptimizer(
            local_evaluator,
            remote_evaluators,
            learning_starts=config["learning_starts"],
            buffer_size=config["buffer_size"],
            prioritized_replay=config["prioritized_replay"],
            prioritized_replay_alpha=config["prioritized_replay_alpha"],
            prioritized_replay_beta=config["prioritized_replay_beta"],
            schedule_max_timesteps=config["schedule_max_timesteps"],
            beta_annealing_fraction=config["beta_annealing_fraction"],
            final_prioritized_replay_beta=config["final_prioritized_replay_beta"],
            prioritized_replay_eps=config["prioritized_replay_eps"],
            train_batch_size=config["train_batch_size"],
            sample_batch_size=config["sample_batch_size"],
            **config["optimizer"]
        )


def check_config_and_setup_param_noise(config):
    """Update the config based on settings.

    Rewrites sample_batch_size to take into account n_step truncation, and also
    adds the necessary callbacks to support parameter space noise exploration.
    """

    # Update effective batch size to include n-step
    adjusted_batch_size = max(config["sample_batch_size"],
                              config.get("n_step", 1))
    config["sample_batch_size"] = adjusted_batch_size

    if config.get("parameter_noise", False):
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("Exploration with parameter space noise requires "
                             "batch_mode to be complete_episodes.")
        if config.get("noisy", False):
            raise ValueError(
                "Exploration with parameter space noise and noisy network "
                "cannot be used at the same time.")
        if config["callbacks"]["on_episode_start"]:
            start_callback = config["callbacks"]["on_episode_start"]
        else:
            start_callback = None

        def on_episode_start(info):
            # as a callback function to sample and pose parameter space
            # noise on the parameters of network
            policies = info["policy"]
            for pi in policies.values():
                pi.add_parameter_noise()
            if start_callback:
                start_callback(info)

        config["callbacks"]["on_episode_start"] = on_episode_start
        if config["callbacks"]["on_episode_end"]:
            end_callback = config["callbacks"]["on_episode_end"]
        else:
            end_callback = None

        def on_episode_end(info):
            # as a callback function to monitor the distance
            # between noisy policy and original policy
            policies = info["policy"]
            episode = info["episode"]
            model = policies[DEFAULT_POLICY_ID].model
            if hasattr(model, "pi_distance"):
                episode.custom_metrics["policy_distance"] = model.pi_distance
            if end_callback:
                end_callback(info)

        config["callbacks"]["on_episode_end"] = on_episode_end


def get_initial_state(config):
    return {
        "last_target_update_ts": 0,
        "num_target_updates": 0,
    }


def make_exploration_schedule(config, worker_index):
    # Use either a different `eps` per worker, or a linear schedule.
    if config["per_worker_exploration"]:
        assert config["num_workers"] > 1, \
            "This requires multiple workers"
        if worker_index >= 0:
            # Exploration constants from the Ape-X paper
            exponent = (
                1 + worker_index / float(config["num_workers"] - 1) * 7)
            return ConstantSchedule(0.4 ** exponent)
        else:
            # local ev should have zero exploration so that eval rollouts
            # run properly
            return ConstantSchedule(0.0)
    return LinearSchedule(
        schedule_timesteps=int(
            config["exploration_fraction"] * config["schedule_max_timesteps"]),
        initial_p=1.0,
        final_p=config["exploration_final_eps"])


def setup_exploration(trainer):
    trainer.exploration0 = make_exploration_schedule(trainer.config, -1)
    trainer.explorations = [
        make_exploration_schedule(trainer.config, i)
        for i in range(trainer.config["num_workers"])
    ]


def update_worker_explorations(trainer):
    global_timestep = trainer.optimizer.num_steps_sampled
    exp_vals = [trainer.exploration0.value(global_timestep)]
    if using_ray_8():
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.set_epsilon(exp_vals[0]))
        for i, e in enumerate(trainer.workers.remote_workers()):
            exp_val = trainer.explorations[i].value(global_timestep)
            e.foreach_trainable_policy.remote(lambda p, _: p.set_epsilon(exp_val))
            exp_vals.append(exp_val)
    else:
        trainer.local_evaluator.foreach_trainable_policy(
            lambda p, _: p.set_epsilon(exp_vals[0]))
        for i, e in enumerate(trainer.remote_evaluators):
            exp_val = trainer.explorations[i].value(global_timestep)
            e.foreach_trainable_policy.remote(lambda p, _: p.set_epsilon(exp_val))
            exp_vals.append(exp_val)
    trainer.train_start_timestep = global_timestep
    trainer.cur_exp_vals = exp_vals


def add_trainer_metrics(trainer, result):
    global_timestep = trainer.optimizer.num_steps_sampled
    result.update(
        timesteps_this_iter=global_timestep - trainer.train_start_timestep,
        info=dict({
            "min_exploration": min(trainer.cur_exp_vals),
            "max_exploration": max(trainer.cur_exp_vals),
            "num_target_updates": trainer.state["num_target_updates"],
        }, **trainer.optimizer.stats()))


def update_target_if_needed(trainer, fetches):
    global_timestep = trainer.optimizer.num_steps_sampled
    if global_timestep - trainer.state["last_target_update_ts"] > \
        trainer.config["target_network_update_freq"]:
        if using_ray_8():
            trainer.workers.local_worker().foreach_trainable_policy(
                lambda p, _: p.update_target())
        else:
            trainer.local_evaluator.foreach_trainable_policy(
                lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = global_timestep
        trainer.state["num_target_updates"] += 1


def collect_metrics(trainer):
    if trainer.config["per_worker_exploration"]:
        # Only collect metrics from the third of workers with lowest eps
        result = trainer.collect_metrics(
            selected_workers=trainer.workers.remote_workers()[
                             -len(trainer.workers.remote_workers()) // 3:])
    else:
        result = trainer.collect_metrics()
    return result


def disable_exploration(trainer):
    trainer.evaluation_workers.local_worker().foreach_trainable_policy(
        lambda p, _: p.set_epsilon(0))


GenericOffPolicyTrainer = build_trainer(
    name="GenericOffPolicyAlgorithm",
    default_policy=None,
    default_config=DEFAULT_CONFIG,
    validate_config=check_config_and_setup_param_noise,
    get_initial_state=get_initial_state,
    make_policy_optimizer=make_optimizer,
    before_init=setup_exploration,
    before_train_step=update_worker_explorations,
    after_optimizer_step=update_target_if_needed,
    after_train_result=add_trainer_metrics,
    collect_metrics_fn=collect_metrics,
    before_evaluate_fn=disable_exploration)


SACTrainer = GenericOffPolicyTrainer.with_updates(
    name="SAC", default_config=DEFAULT_CONFIG, default_policy=SACTFPolicy)
