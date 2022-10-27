import argparse
from dataclasses import dataclass
from enum import Enum
import os.path
import tempfile
import typer
from typing import Optional
import requests


from ray.tune.experiment.config_parser import _make_parser
from ray.tune.result import DEFAULT_RESULTS_DIR


class FrameworkEnum(str, Enum):
    """Supported frameworks for RLlib, used for CLI argument validation."""

    tf = "tf"
    tf2 = "tf2"
    torch = "torch"


class SupportedFileType(str, Enum):
    """Supported file types for RLlib, used for CLI argument validation."""

    yaml = "yaml"
    json = "json"
    python = "python"


def _create_tune_parser_help():
    """Create a Tune dummy parser to access its 'help' docstrings."""
    parser = _make_parser(
        parser_creator=None,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    return parser.__dict__.get("_option_string_actions")


PARSER_HELP = _create_tune_parser_help()


def download_example_file(
    example_file: str,
    base_url: Optional[str] = "https://raw.githubusercontent.com/"
    + "ray-project/ray/master/rllib/",
):
    """Download the example file (e.g. from GitHub) if it doesn't exist locally.
    If the provided example file exists locally, we return it directly.

    Not every user will have cloned our repo and cd'ed into this working directory
    when using the CLI.

    Args:
        example_file: The example file to download.
        base_url: The base URL to download the example file from. Use this if
            'example_file' is a link relative to this base URL. If set to 'None',
            'example_file' is assumed to be a complete URL (or a local file, in which
            case nothing is downloaded).
    """
    temp_file = None
    if not os.path.exists(example_file):

        example_url = base_url + example_file if base_url else example_file
        print(f">>> Attempting to download example file {example_url}...")

        temp_file = tempfile.NamedTemporaryFile()

        r = requests.get(example_url)
        with open(temp_file.name, "wb") as f:
            print(r.content)
            f.write(r.content)

        print(f"  Status code: {r.status_code}")
        if r.status_code == 200:
            print(f"  Downloaded example file to {temp_file.name}")
            # only overwrite the file if the download was successful
            example_file = temp_file.name

    return example_file, temp_file


def get_help(key: str) -> str:
    """Get the help string from a parser for a given key.
    If e.g. 'resource_group' is provided, we return
    the entry for '--resource-group'."""
    key = "--" + key
    key = key.replace("_", "-")
    if key not in PARSER_HELP.keys():
        raise ValueError(f"Key {key} not found in parser.")
    return PARSER_HELP.get(key).help


example_help = dict(
    filter="Filter examples by exact substring match. For instance,"
    " --filter=ppo will only show examples that"
    " contain the substring 'ppo' in their ID. The same way, -f=recsys"
    "will return all recommender system examples.",
)


train_help = dict(
    env="The environment specifier to use. This could be an openAI gym "
    "specifier (e.g. `CartPole-v1`) or a full class-path (e.g. "
    "`ray.rllib.examples.env.simple_corridor.SimpleCorridor`).",
    config_file="Use the algorithm configuration from this file.",
    filetype="The file type of the config file. Defaults to 'yaml' and can also be "
    "'json', or 'python'.",
    experiment_name="Name of the subdirectory under `local_dir` to put results in.",
    framework="The identifier of the deep learning framework you want to use."
    "Choose between TensorFlow 1.x ('tf'), TensorFlow 2.x ('tf2'), "
    "and PyTorch ('torch').",
    v="Whether to use INFO level logging.",
    vv="Whether to use DEBUG level logging.",
    resume="Whether to attempt to resume from previous experiments.",
    local_dir=f"Local dir to save training results to. "
    f"Defaults to '{DEFAULT_RESULTS_DIR}'.",
    local_mode="Run Ray in local mode for easier debugging.",
    ray_address="Connect to an existing Ray cluster at this address instead "
    "of starting a new one.",
    ray_ui="Whether to enable the Ray web UI.",
    ray_num_cpus="The '--num-cpus' argument to use if starting a new cluster.",
    ray_num_gpus="The '--num-gpus' argument to use if starting a new cluster.",
    ray_num_nodes="Emulate multiple cluster nodes for debugging.",
    ray_object_store_memory="--object-store-memory to use if starting a new cluster.",
    upload_dir="Optional URI to sync training results to (e.g. s3://bucket).",
    trace="Whether to attempt to enable tracing for eager mode.",
    torch="Whether to use PyTorch (instead of tf) as the DL framework. "
    "This argument is deprecated, please use --framework to select 'torch'"
    "as backend.",
    eager="Whether to attempt to enable TensorFlow eager execution. "
    "This argument is deprecated, please choose 'tf2' in "
    "--framework to run select eager mode.",
)


eval_help = dict(
    checkpoint="Optional checkpoint from which to roll out. If none provided, we will "
    "evaluate an untrained algorithm.",
    algo="The algorithm or model to train. This may refer to the name of a built-in "
    "Algorithm (e.g. RLlib's `DQN` or `PPO`), or a user-defined trainable "
    "function or class registered in the Tune registry.",
    env="The environment specifier to use. This could be an openAI gym "
    "specifier (e.g. `CartPole-v1`) or a full class-path (e.g. "
    "`ray.rllib.examples.env.simple_corridor.SimpleCorridor`).",
    local_mode="Run Ray in local mode for easier debugging.",
    render="Render the environment while evaluating. Off by default",
    video_dir="Specifies the directory into which videos of all episode"
    "rollouts will be stored.",
    steps="Number of time-steps to roll out. The evaluation will also stop if "
    "`--episodes` limit is reached first. A value of 0 means no "
    "limitation on the number of time-steps run.",
    episodes="Number of complete episodes to roll out. The evaluation will also stop "
    "if `--steps` (time-steps) limit is reached first. A value of 0 means "
    "no limitation on the number of episodes run.",
    out="Output filename",
    config="Algorithm-specific configuration (e.g. `env`, `framework` etc.). "
    "Gets merged with loaded configuration from checkpoint file and "
    "`evaluation_config` settings therein.",
    save_info="Save the info field generated by the step() method, "
    "as well as the action, observations, rewards and done fields.",
    use_shelve="Save rollouts into a Python shelf file (will save each episode "
    "as it is generated). An output filename must be set using --out.",
    track_progress="Write progress to a temporary file (updated "
    "after each episode). An output filename must be set using --out; "
    "the progress file will live in the same folder.",
)


@dataclass
class CLIArguments:
    """Dataclass for CLI arguments and options. We use this class to keep track
    of common arguments, like "run" or "env" that would otherwise be duplicated."""

    # Common arguments
    Algo = typer.Option(None, "--algo", "--run", "-a", "-r", help=get_help("run"))
    AlgoRequired = typer.Option(
        ..., "--algo", "--run", "-a", "-r", help=get_help("run")
    )
    Env = typer.Option(None, "--env", "-e", help=train_help.get("env"))
    EnvRequired = typer.Option(..., "--env", "-e", help=train_help.get("env"))
    Config = typer.Option("{}", "--config", "-c", help=get_help("config"))
    ConfigRequired = typer.Option(..., "--config", "-c", help=get_help("config"))

    # Train arguments
    ConfigFile = typer.Argument(  # config file is now mandatory for "file" subcommand
        ..., help=train_help.get("config_file")
    )
    FileType = typer.Option(
        SupportedFileType.yaml, "--type", "-t", help=train_help.get("filetype")
    )
    Stop = typer.Option("{}", "--stop", "-s", help=get_help("stop"))
    ExperimentName = typer.Option(
        "default", "--experiment-name", "-n", help=train_help.get("experiment_name")
    )
    V = typer.Option(False, "--log-info", "-v", help=train_help.get("v"))
    VV = typer.Option(False, "--log-debug", "-vv", help=train_help.get("vv"))
    Resume = typer.Option(False, help=train_help.get("resume"))
    NumSamples = typer.Option(1, help=get_help("num_samples"))
    CheckpointFreq = typer.Option(0, help=get_help("checkpoint_freq"))
    CheckpointAtEnd = typer.Option(False, help=get_help("checkpoint_at_end"))
    LocalDir = typer.Option(DEFAULT_RESULTS_DIR, help=train_help.get("local_dir"))
    Restore = typer.Option(None, help=get_help("restore"))
    Framework = typer.Option(None, help=train_help.get("framework"))
    ResourcesPerTrial = typer.Option(None, help=get_help("resources_per_trial"))
    KeepCheckpointsNum = typer.Option(None, help=get_help("keep_checkpoints_num"))
    CheckpointScoreAttr = typer.Option(
        "training_iteration", help=get_help("sync_on_checkpoint")
    )
    UploadDir = typer.Option("", help=train_help.get("upload_dir"))
    Trace = typer.Option(False, help=train_help.get("trace"))
    LocalMode = typer.Option(False, help=train_help.get("local_mode"))
    Scheduler = typer.Option("FIFO", help=get_help("scheduler"))
    SchedulerConfig = typer.Option("{}", help=get_help("scheduler_config"))
    RayAddress = typer.Option(None, help=train_help.get("ray_address"))
    RayUi = typer.Option(False, help=train_help.get("ray_ui"))
    RayNumCpus = typer.Option(None, help=train_help.get("ray_num_cpus"))
    RayNumGpus = typer.Option(None, help=train_help.get("ray_num_gpus"))
    RayNumNodes = typer.Option(None, help=train_help.get("ray_num_nodes"))
    RayObjectStoreMemory = typer.Option(
        None, help=train_help.get("ray_object_store_memory")
    )

    # Eval arguments
    Checkpoint = typer.Argument(None, help=eval_help.get("checkpoint"))
    Render = typer.Option(False, help=eval_help.get("render"))
    Steps = typer.Option(10000, help=eval_help.get("steps"))
    Episodes = typer.Option(0, help=eval_help.get("episodes"))
    Out = typer.Option(None, help=eval_help.get("out"))
    SaveInfo = typer.Option(False, help=eval_help.get("save_info"))
    UseShelve = typer.Option(False, help=eval_help.get("use_shelve"))
    TrackProgress = typer.Option(False, help=eval_help.get("track_progress"))


# Note that the IDs of these examples are lexicographically sorted by environment,
# not by algorithm. This should be more natural for users, but could be changed easily.
EXAMPLES = {
    # A2C
    "atari-a2c": {
        "file": "tuned_examples/a2c/atari-a2c.yaml",
        "description": "Runs grid search over several Atari games on A2C.",
    },
    "cartpole-a2c": {
        "file": "tuned_examples/a2c/cartpole_a2c.py",
        "file_type": SupportedFileType.python,
        "description": "Runs A2C on the CartPole-v1 environment.",
    },
    "cartpole-a2c-micro": {
        "file": "tuned_examples/a2c/cartpole-a2c-microbatch.yaml",
        "description": "Runs A2C on the CartPole-v1 environment, using micro-batches.",
    },
    # A3C
    "cartpole-a3c": {
        "file": "tuned_examples/a3c/cartpole-a3c.yaml",
        "description": "Runs A3C on the CartPole-v1 environment.",
    },
    "pong-a3c": {
        "file": "tuned_examples/a3c/pong-a3c.yaml",
        "description": "Runs A3C on the PongDeterministic-v4 environment.",
    },
    # AlphaStar
    "multi-agent-cartpole-alpha-star": {
        "file": "tuned_examples/alpha_star/multi-agent-cartpole-alpha-star.yaml",
        "description": "Runs AlphaStar on 4 CartPole agents.",
    },
    # AlphaZero
    "cartpole-alpha-zero": {
        "file": "tuned_examples/alpha_zero/cartpole-sparse-rewards-alpha-zero.yaml",
        "description": "Runs AlphaZero on a Cartpole with sparse rewards.",
    },
    # Apex DDPG
    "mountaincar-apex-ddpg": {
        "file": "tuned_examples/apex_ddpg/mountaincarcontinuous-apex-ddpg.yaml",
        "description": "Runs Apex DDPG on MountainCarContinuous-v0.",
    },
    "pendulum-apex-ddpg": {
        "file": "tuned_examples/apex_ddpg/pendulum-apex-ddpg.yaml",
        "description": "Runs Apex DDPG on Pendulum-v1.",
    },
    # Apex DQN
    "breakout-apex-dqn": {
        "file": "tuned_examples/apex_dqn/atari-apex-dqn.yaml",
        "description": "Runs Apex DQN on BreakoutNoFrameskip-v4.",
    },
    "cartpole-apex-dqn": {
        "file": "tuned_examples/apex_dqn/cartpole-apex-dqn.yaml",
        "description": "Runs Apex DQN on CartPole-v1.",
    },
    "pong-apex-dqn": {
        "file": "tuned_examples/apex_dqn/pong-apex-dqn.yaml",
        "description": "Runs Apex DQN on PongNoFrameskip-v4.",
    },
    # APPO
    "cartpole-appo": {
        "file": "tuned_examples/appo/cartpole-appo.yaml",
        "description": "Runs APPO on CartPole-v1.",
    },
    "frozenlake-appo": {
        "file": "tuned_examples/appo/frozenlake-appo-vtrace.yaml",
        "description": "Runs APPO on FrozenLake-v1.",
    },
    "halfcheetah-appo": {
        "file": "tuned_examples/appo/halfcheetah-appo.yaml",
        "description": "Runs APPO on HalfCheetah-v2.",
    },
    "multi-agent-cartpole-appo": {
        "file": "tuned_examples/appo/multi-agent-cartpole-appo.yaml",
        "description": "Runs APPO on RLlib's MultiAgentCartPole",
    },
    "pendulum-appo": {
        "file": "tuned_examples/appo/pendulum-appo.yaml",
        "description": "Runs APPO on Pendulum-v1.",
    },
    "pong-appo": {
        "file": "tuned_examples/appo/pong-appo.yaml",
        "description": "Runs APPO on PongNoFrameskip-v4.",
    },
    # ARS
    "cartpole-ars": {
        "file": "tuned_examples/ars/cartpole-ars.yaml",
        "description": "Runs ARS on CartPole-v1.",
    },
    "swimmer-ars": {
        "file": "tuned_examples/ars/swimmer-ars.yaml",
        "description": "Runs ARS on Swimmer-v2.",
    },
    # Bandits
    "recsys-bandits": {
        "file": "tuned_examples/bandits/"
        + "interest-evolution-recsim-env-bandit-linucb.yaml",
        "description": "Runs BanditLinUCB on a Recommendation Simulation environment.",
    },
    # BC
    "cartpole-bc": {
        "file": "tuned_examples/bc/cartpole-bc.yaml",
        "description": "Runs BC on CartPole-v1.",
    },
    # CQL
    "halfcheetah-cql": {
        "file": "tuned_examples/cql/halfcheetah-cql.yaml",
        "description": "Runs grid search on HalfCheetah environments with CQL.",
    },
    "hopper-cql": {
        "file": "tuned_examples/cql/hopper-cql.yaml",
        "description": "Runs grid search on Hopper environments with CQL.",
    },
    "pendulum-cql": {
        "file": "tuned_examples/cql/pendulum-cql.yaml",
        "description": "Runs CQL on Pendulum-v1.",
    },
    # CRR
    "cartpole-crr": {
        "file": "tuned_examples/crr/CartPole-v1-crr.yaml",
        "description": "Run CRR on CartPole-v1.",
    },
    "pendulum-crr": {
        "file": "tuned_examples/crr/pendulum-v1-crr.yaml",
        "description": "Run CRR on Pendulum-v1.",
    },
    # DDPG
    "halfcheetah-ddpg": {
        "file": "tuned_examples/ddpg/halfcheetah-ddpg.yaml",
        "description": "Runs DDPG on HalfCheetah-v2.",
    },
    "halfcheetah-bullet-ddpg": {
        "file": "tuned_examples/ddpg/halfcheetah-pybullet-ddpg.yaml",
        "description": "Runs DDPG on HalfCheetahBulletEnv-v0.",
    },
    "hopper-bullet-ddpg": {
        "file": "tuned_examples/ddpg/hopper-pybullet-ddpg.yaml",
        "description": "Runs DDPG on HopperBulletEnv-v0.",
    },
    "mountaincar-ddpg": {
        "file": "tuned_examples/ddpg/mountaincarcontinuous-ddpg.yaml",
        "description": "Runs DDPG on MountainCarContinuous-v0.",
    },
    "pendulum-ddpg": {
        "file": "tuned_examples/ddpg/pendulum-ddpg.yaml",
        "description": "Runs DDPG on Pendulum-v1.",
    },
    # DDPPO
    "breakout-ddppo": {
        "file": "tuned_examples/ddppo/atari-ddppo.yaml",
        "description": "Runs DDPPO on BreakoutNoFrameskip-v4.",
    },
    "cartpole-ddppo": {
        "file": "tuned_examples/ddppo/cartpole-ddppo.yaml",
        "description": "Runs DDPPO on CartPole-v1",
    },
    "pendulum-ddppo": {
        "file": "tuned_examples/ddppo/pendulum-ddppo.yaml",
        "description": "Runs DDPPO on Pendulum-v1.",
    },
    # DQN
    "atari-dqn": {
        "file": "tuned_examples/dqn/atari-dqn.yaml",
        "description": "Run grid search on Atari environments with DQN.",
    },
    "atari-duel-ddqn": {
        "file": "tuned_examples/dqn/atari-duel-ddqn.yaml",
        "description": "Run grid search on Atari environments "
        "with duelling double DQN.",
    },
    "cartpole-dqn": {
        "file": "tuned_examples/dqn/cartpole-dqn.yaml",
        "description": "Run DQN on CartPole-v1.",
    },
    "pong-dqn": {
        "file": "tuned_examples/dqn/pong-dqn.yaml",
        "description": "Run DQN on PongDeterministic-v4.",
    },
    "pong-rainbow": {
        "file": "tuned_examples/dqn/pong-rainbow.yaml",
        "description": "Run Rainbow on PongDeterministic-v4.",
    },
    # DREAMER
    "dm-control-dreamer": {
        "file": "tuned_examples/dreamer/dreamer-deepmind-control.yaml",
        "description": "Run DREAMER on a suite of control problems by Deepmind.",
    },
    # DT
    "cartpole-dt": {
        "file": "tuned_examples/dt/CartPole-v1-dt.yaml",
        "description": "Run DT on CartPole-v1.",
    },
    "pendulum-dt": {
        "file": "tuned_examples/dt/pendulum-v1-dt.yaml",
        "description": "Run DT on Pendulum-v1.",
    },
    # ES
    "cartpole-es": {
        "file": "tuned_examples/es/cartpole-es.yaml",
        "description": "Run ES on CartPole-v1.",
    },
    "humanoid-es": {
        "file": "tuned_examples/es/humanoid-es.yaml",
        "description": "Run ES on Humanoid-v2.",
    },
    # IMPALA
    "atari-impala": {
        "file": "tuned_examples/impala/atari-impala.yaml",
        "description": "Run grid search over several atari games with IMPALA.",
    },
    "cartpole-impala": {
        "file": "tuned_examples/impala/cartpole-impala.yaml",
        "description": "Run IMPALA on CartPole-v1.",
    },
    "multi-agent-cartpole-impala": {
        "file": "tuned_examples/impala/multi-agent-cartpole-impala.yaml",
        "description": "Run IMPALA on RLlib's MultiAgentCartPole",
    },
    "pendulum-impala": {
        "file": "tuned_examples/impala/pendulum-impala.yaml",
        "description": "Run IMPALA on Pendulum-v1.",
    },
    "pong-impala": {
        "file": "tuned_examples/impala/pong-impala-fast.yaml",
        "description": "Run IMPALA on PongNoFrameskip-v4.",
    },
    # MADDPG
    "two-step-game-maddpg": {
        "file": "tuned_examples/maddpg/two-step-game-maddpg.yaml",
        "description": "Run RLlib's Two-step game with multi-agent DDPG.",
    },
    # MAML
    "cartpole-maml": {
        "file": "tuned_examples/maml/cartpole-maml.yaml",
        "description": "Run MAML on CartPole-v1.",
    },
    "halfcheetah-maml": {
        "file": "tuned_examples/maml/halfcheetah-rand-direc-maml.yaml",
        "description": "Run MAML on a custom HalfCheetah environment.",
    },
    "pendulum-maml": {
        "file": "tuned_examples/maml/pendulum-mass-maml.yaml",
        "description": "Run MAML on a custom Pendulum environment.",
    },
    # MARWIL
    "cartpole-marwil": {
        "file": "tuned_examples/marwil/cartpole-marwil.yaml",
        "description": "Run MARWIL on CartPole-v1.",
    },
    # MBMPO
    "cartpole-mbmpo": {
        "file": "tuned_examples/mbmpo/cartpole-mbmpo.yaml",
        "description": "Run MBMPO on a CartPole environment wrapper.",
    },
    "halfcheetah-mbmpo": {
        "file": "tuned_examples/mbmpo/halfcheetah-mbmpo.yaml",
        "description": "Run MBMPO on a HalfCheetah environment wrapper.",
    },
    "hopper-mbmpo": {
        "file": "tuned_examples/mbmpo/hopper-mbmpo.yaml",
        "description": "Run MBMPO on a Hopper environment wrapper.",
    },
    "pendulum-mbmpo": {
        "file": "tuned_examples/mbmpo/pendulum-mbmpo.yaml",
        "description": "Run MBMPO on a Pendulum environment wrapper.",
    },
    # PG
    "cartpole-pg": {
        "file": "tuned_examples/pg/cartpole-pg.yaml",
        "description": "Run PG on CartPole-v1",
    },
    # PPO
    "atari-ppo": {
        "file": "tuned_examples/ppo/atari-ppo.yaml",
        "description": "Run grid search over several atari games with PPO.",
    },
    "cartpole-ppo": {
        "file": "tuned_examples/ppo/cartpole-ppo.yaml",
        "description": "Run PPO on CartPole-v1.",
    },
    "halfcheetah-ppo": {
        "file": "tuned_examples/ppo/halfcheetah-ppo.yaml",
        "description": "Run PPO on HalfCheetah-v2.",
    },
    "hopper-ppo": {
        "file": "tuned_examples/ppo/hopper-ppo.yaml",
        "description": "Run PPO on Hopper-v1.",
    },
    "humanoid-ppo": {
        "file": "tuned_examples/ppo/humanoid-ppo.yaml",
        "description": "Run PPO on Humanoid-v1.",
    },
    "pendulum-ppo": {
        "file": "tuned_examples/ppo/pendulum-ppo.yaml",
        "description": "Run PPO on Pendulum-v1.",
    },
    "pong-ppo": {
        "file": "tuned_examples/ppo/pong-ppo.yaml",
        "description": "Run PPO on PongNoFrameskip-v4.",
    },
    "recsys-ppo": {
        "file": "tuned_examples/ppo/recomm-sys001-ppo.yaml",
        "description": "Run PPO on a recommender system example from RLlib.",
    },
    "repeatafterme-ppo": {
        "file": "tuned_examples/ppo/repeatafterme-ppo-lstm.yaml",
        "description": "Run PPO on RLlib's RepeatAfterMe environment.",
    },
    "walker2d-ppo": {
        "file": "tuned_examples/ppo/walker2d-ppo.yaml",
        "description": "Run PPO on the Walker2d-v1 environment.",
    },
    # QMIX
    "two-step-game-qmix": {
        "file": "tuned_examples/qmix/two-step-game-qmix.yaml",
        "description": "Run QMIX on RLlib's two-step game.",
    },
    # R2D2
    "stateless-cartpole-r2d2": {
        "file": "tuned_examples/r2d2/stateless-cartpole-r2d2.yaml",
        "description": "Run R2D2 on a stateless cart pole environment.",
    },
    # SAC
    "atari-sac": {
        "file": "tuned_examples/sac/atari-sac.yaml",
        "description": "Run grid search on several atari games with SAC.",
    },
    "cartpole-sac": {
        "file": "tuned_examples/sac/cartpole-sac.yaml",
        "description": "Run SAC on CartPole-v1",
    },
    "halfcheetah-sac": {
        "file": "tuned_examples/sac/halfcheetah-sac.yaml",
        "description": "Run SAC on HalfCheetah-v3.",
    },
    "pacman-sac": {
        "file": "tuned_examples/sac/mspacman-sac.yaml",
        "description": "Run SAC on MsPacmanNoFrameskip-v4.",
    },
    "pendulum-sac": {
        "file": "tuned_examples/sac/pendulum-sac.yaml",
        "description": "Run SAC on Pendulum-v1.",
    },
    # SimpleQ
    "cartpole-simpleq": {
        "file": "tuned_examples/simple_q/cartpole-simpleq.yaml",
        "description": "Run SimpleQ on CartPole-v1",
    },
    # SlateQ
    "recsys-long-term-slateq": {
        "file": "tuned_examples/slateq/long-term-satisfaction-recsim-env-slateq.yaml",
        "description": "Run SlateQ on a recommendation system aimed at "
        "long-term satisfaction.",
    },
    "recsys-parametric-slateq": {
        "file": "tuned_examples/slateq/parametric-item-reco-env-slateq.yaml",
        "description": "SlateQ run on a recommendation system.",
    },
    "recsys-slateq": {
        "file": "tuned_examples/slateq/recomm-sys001-slateq.yaml",
        "description": "SlateQ run on a recommendation system.",
    },
    # TD3
    "inverted-pendulum-td3": {
        "file": "tuned_examples/td3/invertedpendulum-td3.yaml",
        "description": "Run TD3 on InvertedPendulum-v2.",
    },
    "mujoco-td3": {
        "file": "tuned_examples/td3/mujoco-td3.yaml",
        "description": "Run TD3 against four of the hardest MuJoCo tasks.",
    },
    "pendulum-td3": {
        "file": "tuned_examples/td3/pendulum-td3.yaml",
        "description": "Run TD3 on Pendulum-v1.",
    },
}
