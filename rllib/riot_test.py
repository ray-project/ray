# Third Party
import numpy as np
from gym import spaces

from ray.rllib.examples.env.random_env import RandomMultiAgentEnv
#from ray.rllib.agents.impala import ImpalaTrainer

OBSERVATION_SPACE_DICT = spaces.Dict(
    {
        "action_mask": spaces.Box(low=0, high=1, shape=(119,), dtype=np.int32),
        "board": spaces.Box(low=0, high=156, shape=(1, 4, 7), dtype=np.int32),#200
        "buy_menu": spaces.Box(low=0, high=5, shape=(1, 52), dtype=np.int32),
        "current_player_trait_all": spaces.Box(low=0, high=1, shape=(1, 42), dtype=np.int32),
        "current_player_trait_deployment": spaces.Box(low=0, high=1, shape=(1, 42), dtype=np.int32),
        "current_player_unit_counts_combined": spaces.Box(
            low=0, high=40, shape=(1, 104), dtype=np.int32
        ),
        "current_player_unit_deployment": spaces.Box(
            low=0, high=0, shape=(1, 18), dtype=np.int32
        ),  # Always zero?
        "experience": spaces.Box(low=0, high=65, shape=(1, 1), dtype=np.int32),#100
        "players_0_gold": spaces.Box(low=0, high=100, shape=(1, 1), dtype=np.int32),
        "players_0_health": spaces.Box(low=0, high=100, shape=(1, 1), dtype=np.int32),
        "players_0_level": spaces.Box(low=1, high=9, shape=(1, 1), dtype=np.int32),#10
        "players_0_unit_counts": spaces.Box(low=0, high=20, shape=(1, 52), dtype=np.int32),
        "players_0_units": spaces.Box(low=0, high=156, shape=(1, 18), dtype=np.int32),#1000
        "players_0_units_traits": spaces.Box(low=0, high=1, shape=(1, 414), dtype=np.int32),
        "players_1_gold": spaces.Box(low=0, high=100, shape=(1, 1), dtype=np.int32),
        "players_1_health": spaces.Box(low=0, high=100, shape=(1, 1), dtype=np.int32),
        "players_1_level": spaces.Box(low=1, high=9, shape=(1, 1), dtype=np.int32),#10
        "players_1_unit_counts": spaces.Box(low=0, high=20, shape=(1, 52), dtype=np.int32),
        "players_1_units": spaces.Box(low=0, high=156, shape=(1, 18), dtype=np.int32),#1000
        "players_1_units_traits": spaces.Box(low=0, high=1, shape=(1, 414), dtype=np.int32),
        "round": spaces.Box(low=1, high=50, shape=(1, 1), dtype=np.int32),
    }
)

ACTION_SPACE = spaces.Discrete(119)

# Standard Library
import pickle
import pprint
import random

# Third Party
import numpy as np
import ray
#from absl import app
#from absl import flags
from ray import tune
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import _unpack_obs
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.framework import try_import_tf
from ray.tune import CLIReporter
#from ray.tune.integration.wandb import WandbLoggerCallback
#from ray.tune.registry import register_env
#from ray.tune.schedulers import ASHAScheduler

# Project
#from tft_env_rllib import tft

tf1, tf, tfv = try_import_tf()

#FLAGS = flags.FLAGS
#flags.DEFINE_string("wandb_api_key", None, "Personal Weights and Biases API key.")


def upload_combat_model(model_name):
    with open(f"tft_env_rllib/{model_name}--network_config.pkl", "rb") as input_io:
        network_config = pickle.load(input_io)

    with open(f"tft_env_rllib/{model_name}--weights.pkl", "rb") as input_io:
        weights = pickle.load(input_io)

    return {
        "combat_model_config_ref": ray.put(network_config),
        "combat_model_weights_ref": ray.put(weights),
    }


def upload_riot_rl_ppo_policy_network(policy_name):
    with open(f"riot_rl_policies/{policy_name}--policy_network_config.pkl", "rb") as input_io:
        policy_network_config = pickle.load(input_io)

    with open(f"riot_rl_policies/{policy_name}--policy_network_weights.pkl", "rb") as input_io:
        policy_network_weights = pickle.load(input_io)

    return {
        "policy_network_config_ref": ray.put(policy_network_config),
        "policy_network_weights_ref": ray.put(policy_network_weights),
    }


class MaskedRandomPolicy(RandomPolicy):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        **kwargs,
    ):
        unpacked_obs = _unpack_obs(
            np.array(obs_batch, dtype=np.float32),
            self.observation_space.original_space,
            tensorlib=np,
        )

        actions = []
        for action_mask in unpacked_obs["action_mask"]:
            available_actions = [i for i, allowed in enumerate(action_mask) if allowed]
            actions.append(random.choice(available_actions))

        return np.array(actions), [], {}


class RiotRLTFTModel(TFModelV2):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, None, model_config, name)

        self.num_outputs = num_outputs

        inputs, concat_layer, action_mask = self._get_network_body()

        masked_policy_logits = self._build_policy_network(concat_layer, action_mask)
        value = self._build_value_network(concat_layer)

        self.logits_and_value_model = tf.keras.models.Model(
            [inputs, action_mask], [masked_policy_logits, value]
        )

    def _get_network_body(self):
        shapes = {
            input_name: self.obs_space.original_space[input_name].shape
            for input_name in self.obs_space.original_space
        }

        action_mask = tf.keras.layers.Input(shape=shapes["action_mask"], name="action_mask")

        unit_embedding = tf.keras.layers.Embedding(
            input_dim=157,  # input_dim=tft_utils.NUM_UNITS + 1
            output_dim=10,
            name="unit_embedding",
        )
        unit_trait_shared = tf.keras.layers.Dense(20, activation="relu")
        # could go all the way up to pool size but use 3* +1 from carousel for now

        # NOTE to RLlib folks: These embeddings of scalars are weird to me; there's an explanation
        # that I don't really buy, but it works on the current platform so I'm keeping my head in
        # the sand for now.
        gold_embedding = tf.keras.layers.Embedding(
            input_dim=101, output_dim=8, name="gold_embedding"  # input_dim=constants.MAX_GOLD + 1
        )
        health_embedding = tf.keras.layers.Embedding(
            input_dim=101,  # input_dim=constants.MAX_HEALTH + 1
            output_dim=4,
            name="health_embedding",
        )
        level_embedding = tf.keras.layers.Embedding(
            input_dim=10, output_dim=2, name="level_embedding"  # input_dim=constants.MAX_LEVEL + 1
        )
        experience_embedding = tf.keras.layers.Embedding(
            input_dim=66,  # input_dim=max(constants.exp_to_level_up.values()),
            output_dim=4,
            name="experience_embedding",
        )

        buy_menu_input = tf.keras.layers.Input(
            shape=shapes["current_player_unit_counts_combined"],
            name="current_player_unit_counts_combined",
        )
        buy_menu_reshape = tf.keras.layers.Reshape((1, 52, 2))(buy_menu_input)
        experience_input = tf.keras.layers.Input(shape=shapes["experience"], name="experience")
        round_input = tf.keras.layers.Input(shape=shapes["round"], name="round")
        deployed_input = tf.keras.layers.Input(
            shape=shapes["current_player_unit_deployment"],
            dtype="float32",
            name="current_player_unit_deployment",
        )
        deployed_trait_input = tf.keras.layers.Input(
            shape=shapes["current_player_trait_deployment"],
            dtype="float32",
            name="current_player_trait_deployment",
        )
        all_trait_input = tf.keras.layers.Input(
            shape=shapes["current_player_trait_all"],
            dtype="float32",
            name="current_player_trait_all",
        )
        board_input = tf.keras.layers.Input(shape=shapes["board"], dtype="int32", name="board")

        inputs = [
            buy_menu_input,
            experience_input,
            round_input,
            deployed_input,
            deployed_trait_input,
            all_trait_input,
            board_input,
        ]

        conv_layer_shop = tf.keras.layers.Conv2D(
            filters=10, kernel_size=[1, 1], strides=[1, 1], activation="relu"
        )

        def embed_player(player_index):
            gold_input = tf.keras.layers.Input(
                shape=shapes[f"players_{player_index}_gold"],
                dtype="int32",
                name=f"players_{player_index}_gold",
            )
            health_input = tf.keras.layers.Input(
                shape=shapes[f"players_{player_index}_health"],
                dtype="int32",
                name=f"players_{player_index}_health",
            )
            level_input = tf.keras.layers.Input(
                shape=shapes[f"players_{player_index}_level"],
                dtype="int32",
                name=f"players_{player_index}_level",
            )
            units_input = tf.keras.layers.Input(
                shape=shapes[f"players_{player_index}_units"],
                dtype="int32",
                name=f"players_{player_index}_units",
            )
            units_traits_input = tf.keras.layers.Input(
                shape=shapes[f"players_{player_index}_units_traits"],
                dtype="float32",
                name=f"players_{player_index}_units_traits",
            )

            inputs.extend([gold_input, health_input, level_input, units_input, units_traits_input])

            units_traits_dense = unit_trait_shared(units_traits_input)

            gold_embedded = gold_embedding(gold_input)
            health_embedded = health_embedding(health_input)
            level_embedded = level_embedding(level_input)
            units_embedded = unit_embedding(units_input)

            return tf.keras.layers.Concatenate()(
                [
                    tf.keras.layers.Flatten()(gold_embedded),
                    tf.keras.layers.Flatten()(health_embedded),
                    tf.keras.layers.Flatten()(level_embedded),
                    tf.keras.layers.Flatten()(units_embedded),
                    tf.keras.layers.Flatten()(units_traits_dense),
                ]
            )

        players_embedded = [embed_player(i) for i in range(2)]
        experience_embedded = experience_embedding(experience_input)
        shop_info = conv_layer_shop(buy_menu_reshape)
        board_embedded = unit_embedding(board_input)

        concat_layer = tf.keras.layers.Concatenate()(
            [
                tf.keras.layers.Flatten()(shop_info),
                tf.keras.layers.Flatten()(experience_embedded),
                tf.keras.layers.Flatten()(round_input),
                tf.keras.layers.Flatten()(deployed_input),
                tf.keras.layers.Flatten()(deployed_trait_input),
                tf.keras.layers.Flatten()(all_trait_input),
                tf.keras.layers.Flatten()(board_embedded),
            ]
            + players_embedded
        )

        return inputs, concat_layer, action_mask

    def _build_policy_network(self, concat_layer, action_mask, dense_widths=(128, 128)):
        x = concat_layer
        for width in dense_widths:
            x = tf.keras.layers.Dense(width, activation="relu")(x)

        x = tf.keras.layers.Dense(self.num_outputs, activation="linear")(x)
        inf_mask = tf.keras.layers.Lambda(
            lambda y: tf.keras.backend.maximum(tf.keras.backend.log(y), tf.float32.min)
        )(action_mask)
        masked_policy_logits = tf.keras.layers.Add()([x, inf_mask])

        return masked_policy_logits

    def _build_value_network(self, concat_layer, dense_widths=(192, 128, 64)):
        x = concat_layer
        for width in dense_widths:
            x = tf.keras.layers.Dense(width, activation="relu")(x)

        value = tf.keras.layers.Dense(1, name="value", activation="linear")(x)

        return value

    def forward(self, input_dict, state, seq_lens):
        # TODO(bkasper): Achieve compatibility with the model without hard coding input order.
        inputs = [
            input_dict["obs"][input_name]
            for input_name in [
                "current_player_unit_counts_combined",
                "experience",
                "round",
                "current_player_unit_deployment",
                "current_player_trait_deployment",
                "current_player_trait_all",
                "board",
                "players_0_gold",
                "players_0_health",
                "players_0_level",
                "players_0_units",
                "players_0_units_traits",
                "players_1_gold",
                "players_1_health",
                "players_1_level",
                "players_1_units",
                "players_1_units_traits",
            ]
        ]

        action_mask = input_dict["obs"]["action_mask"]
        masked_policy_logits, value = self._get_nn_outputs([inputs, action_mask])

        self._value = tf.reshape(value, [-1])

        return masked_policy_logits, []

    @tf.function
    def _get_nn_outputs(self, nn_inputs):
        masked_policy_logits, value = self.logits_and_value_model(nn_inputs)

        return masked_policy_logits, value

    def value_function(self):
        return self._value


class SelfPlayCallbacks(DefaultCallbacks):
    def __init__(self, winrate_threshold=0.7):
        super().__init__()
        self.num_snapshots = 0

    def on_train_result(self, *, trainer, result, **kwargs):
        if (
            # The first iteration sometimes only includes evaluation results, for some reason.
            "policy_reward_mean" in result
            and "main_agent" in result["policy_reward_mean"]
            # and result["policy_reward_mean"]["main_agent"] > 0.6
            and result["policy_reward_mean"]["main_agent"] > 0.7  # TODO: (sven) <- this should probably be more like 0.95 - 0.98
        ):
            self.num_snapshots += 1
            new_pol_id = f"snapshot_{self.num_snapshots}"
            print(f"Snapshotting and adding new opponent: {new_pol_id}.")

            def policy_mapping_fn(agent_id, episode, **kwargs):
                return (
                    "main_agent"
                    if episode.episode_id % 2 == agent_id or random.random() <= 0.0
                    # else f"snapshot_{random.choice(range(1, self.num_snapshots + 1))}"
                    else f"snapshot_{self.num_snapshots}"
                )

            new_policy = trainer.add_policy(
                policy_id=new_pol_id,
                policy_cls=type(trainer.get_policy("main_agent")),
                config={"model": {"custom_model": "riot_rl_tft_model"}},
                policy_mapping_fn=policy_mapping_fn,
            )

            # Revert evaluation workers' policy_mapping_fn.
            #trainer.evaluation_workers.foreach_worker(
            #    lambda evaluation_worker: evaluation_worker.set_policy_mapping_fn(
            #        evaluation_policy_mapping_fn
            #    )
            #)

            main_state = trainer.get_policy("main_agent").get_state()
            new_policy.set_state(main_state)
            trainer.workers.sync_weights()

        result["snapshots"] = self.num_snapshots


def evaluation_policy_mapping_fn(agent_id, episode, **kwargs):
    return "main_agent"
    # return "main_agent" if episode.episode_id % 2 == agent_id else "random"


if __name__ == "__main__":
    ray.init(local_mode=True)#().connect()

    ModelCatalog.register_custom_model("riot_rl_tft_model", RiotRLTFTModel)
    #register_env(
    #    "TFTRestrictedEnv", lambda kwargs: tft.TFTRestrictedEnv(initial_health=100, **kwargs),
    #)

    # NOTE to RLlib folks: The environment uses a pre-trained "combat model" that approximates
    # results in the combat phase of the game, so that we can avoid using real game servers (for
    # now). This model is uploaded to the object store and the environment imports the pickled model
    # config and weights via `ray.get`. Same for the learned opponent's policy network commented out
    # just below.
    #combat_model_refs = upload_combat_model("tft_noitem_symmetric")
    # opponent_policy_network_refs = upload_riot_rl_ppo_policy_network(
    #     "riot_rl_runs_tft-ppo-28-4-1-main-agent_policies_PPOPolicy-0000000000-latest"
    # )

    # Defaults
    config = {
        # === Basics ===
        "env": RandomMultiAgentEnv,#"TFTRestrictedEnv",
        #"env_config": {"combat_model_refs": combat_model_refs, "opponent": None},
        "env_config": {
            "observation_space": OBSERVATION_SPACE_DICT,
            "action_space": ACTION_SPACE,
        },
        # "framework": "tf",
        "framework": "tf2",
        "model": {"custom_model": "riot_rl_tft_model"},
        #"eager_tracing": True,
        #"min_iter_time_s": 60,
        "num_workers": 2,
        #"num_envs_per_worker": 4,
        # === Multiagent ===
        "multiagent": {
            "policies": {
                "main_agent": PolicySpec(),
                "random": PolicySpec(policy_class=MaskedRandomPolicy),
            },
            "policy_mapping_fn": (
                lambda agent_id, episode, **kwargs: "main_agent"
                if episode.episode_id % 2 == agent_id
                else "random"
            ),
            "policies_to_train": ["main_agent"],
        },
        # === Learning ===
        "rollout_fragment_length": 16,
        "train_batch_size": 256,
        "lr": 1.5e-4,
        "entropy_coeff": 5e-4,
        # === Callbacks ===
        "callbacks": SelfPlayCallbacks,  # TODO(bkasper): functools.partial constructor parameter.
        # === Evaluation ===
        #"evaluation_num_workers": 10,
        #"evaluation_parallel_to_training": True,
        #"evaluation_interval": 1,
        #"evaluation_num_episodes": 10,
        #"evaluation_config": {
        #    "env_config": {
        #        "opponent": "BuyPolicy",
        #        # "opponent": "Policy005",
        #        # "opponent": "RiotRLPPOOpponent",
        #        # "riot_rl_ppo_policy_network_refs": opponent_policy_network_refs,
        #        # "opponent": None,
        #    },
        #    "explore": True,
        #    "multiagent": {"policy_mapping_fn": evaluation_policy_mapping_fn},
        #},
    }

    # Tuning
    #config.update(
    #    {
            # TODO: look into this tomorrow
            #"min_iter_time_s": 300,
            #"num_cpus_for_driver": 0,  # works around 1CPU/1GPU placement group issue in tuning
            #"num_workers": 2,
            #"evaluation_num_workers": 10,
            #"evaluation_num_episodes": 200,
            #"rollout_fragment_length": tune.choice([16, 32, 64, 256, 512]),
            # TODO: (sven) Not sure wether the batch size is simply too large (and
            #  the number of iterations too small. Note that
            #"train_batch_size": tune.sample_from(
            #    lambda spec: spec.config.rollout_fragment_length
            #    * np.random.choice([8, 16, 32, 64, 128])
            #),
            # "clip_rewards": tune.choice([False, True]),
            #"lr": tune.qloguniform(1e-6, 1e-3, 5e-7),
            # "lr_schedule": [[0, 0.0005], [20_000_000, 0.000000000001]],
            #"vf_loss_coeff": tune.quniform(0.01, 0.5, 0.01),
            #"entropy_coeff": tune.quniform(1e-4, 1e-2, 1e-4),
            # # "entropy_coeff_schedule": None,
            # "replay_proportion": tune.choice([0.0, 0.5]),
            # "replay_buffer_num_slots": tune.sample_from(
            #     lambda spec: 256 * np.random.choice([1, 4, 16])
            #     if spec.config.replay_proportion > 0.0
            #     else 0
            # ),
    #    }
    #)

    # ASHA
    #scheduling_config = {
    #    "scheduler": ASHAScheduler(
    #        time_attr="training_iteration",
    #        max_t=36,
    #        grace_period=6,
    #        reduction_factor=3,
    #        brackets=1,
    #    ),
    #    "num_samples": 64,
    #    "metric": "evaluation/policy_reward_mean/main_agent",
    #    "mode": "max",
    #}

    results = tune.run(
        "IMPALA",
        config=config,
        # This seems extremely low for IMPALA, given quite small batch sizes and no
        # minibatch SGD iters (over same train-batch, like PPO does it).
        #stop={"training_iteration": 36},
        #**scheduling_config,
        #callbacks=[
        #    WandbLoggerCallback(
        #        # project="Anyscale-TFTRestrictedEnv-Tune2",
        #        project="Anyscale-TFTRestrictedEnv-Tune3-LastOpponent",
        #        api_key=FLAGS.wandb_api_key,
        #        log_config=True,
        #    )
        #]
        #if FLAGS.wandb_api_key
        #else [],
        progress_reporter=CLIReporter(
            metric_columns={
                "training_iteration": "iter",
                "time_total_s": "time_total_s",
                "timesteps_total": "ts",
                "snapshots": "snapshots",
                "episodes_this_iter": "train_episodes",
                "policy_reward_mean/main_agent": "train_winrate",
                "evaluation/episodes_this_iter": "eval_episodes",
                "evaluation/policy_reward_mean/main_agent": "eval_winrate",
            },
            sort_by_metric=True,
        ),
        # TODO(bkasper): Add checkpointing (checkpoint_frequency).
    )

    print(f"Best hyperparameters were: {pprint.pformat(results.best_config)}")
    results.results_df.to_csv("results.csv")
