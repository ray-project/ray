import argparse
import json

from ray.train import CheckpointConfig
from ray.tune.error import TuneError
from ray.tune.experiment import Trial
from ray.tune.resources import json_to_resources

# For compatibility under py2 to consider unicode as str
from ray.tune.utils.serialization import TuneFunctionEncoder
from ray.tune.utils.util import SafeFallbackEncoder


def _make_parser(parser_creator=None, **kwargs):
    """Returns a base argument parser for the ray.tune tool.

    Args:
        parser_creator: A constructor for the parser class.
        kwargs: Non-positional args to be passed into the
            parser class constructor.
    """

    if parser_creator:
        parser = parser_creator(**kwargs)
    else:
        parser = argparse.ArgumentParser(**kwargs)

    # Note: keep this in sync with rllib/train.py
    parser.add_argument(
        "--run",
        default=None,
        type=str,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLlib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.",
    )
    parser.add_argument(
        "--stop",
        default="{}",
        type=json.loads,
        help="The stopping criteria, specified in JSON. The keys may be any "
        "field returned by 'train()' e.g. "
        '\'{"time_total_s": 600, "training_iteration": 100000}\' to stop '
        "after 600 seconds or 100k iterations, whichever is reached first.",
    )
    parser.add_argument(
        "--config",
        default="{}",
        type=json.loads,
        help="Algorithm-specific configuration (e.g. env, hyperparams), "
        "specified in JSON.",
    )
    parser.add_argument(
        "--resources-per-trial",
        default=None,
        type=json_to_resources,
        help="Override the machine resources to allocate per trial, e.g. "
        '\'{"cpu": 64, "gpu": 8}\'. Note that GPUs will not be assigned '
        "unless you specify them here. For RLlib, you probably want to "
        "leave this alone and use RLlib configs to control parallelism.",
    )
    parser.add_argument(
        "--num-samples",
        default=1,
        type=int,
        help="Number of times to repeat each trial.",
    )
    parser.add_argument(
        "--checkpoint-freq",
        default=0,
        type=int,
        help="How many training iterations between checkpoints. "
        "A value of 0 (default) disables checkpointing.",
    )
    parser.add_argument(
        "--checkpoint-at-end",
        action="store_true",
        help="Whether to checkpoint at the end of the experiment. Default is False.",
    )
    parser.add_argument(
        "--keep-checkpoints-num",
        default=None,
        type=int,
        help="Number of best checkpoints to keep. Others get "
        "deleted. Default (None) keeps all checkpoints.",
    )
    parser.add_argument(
        "--checkpoint-score-attr",
        default="training_iteration",
        type=str,
        help="Specifies by which attribute to rank the best checkpoint. "
        "Default is increasing order. If attribute starts with min- it "
        "will rank attribute in decreasing order. Example: "
        "min-validation_loss",
    )
    parser.add_argument(
        "--export-formats",
        default=None,
        help="List of formats that exported at the end of the experiment. "
        "Default is None. For RLlib, 'checkpoint' and 'model' are "
        "supported for TensorFlow policy graphs.",
    )
    parser.add_argument(
        "--max-failures",
        default=3,
        type=int,
        help="Try to recover a trial from its last checkpoint at least this "
        "many times. Only applies if checkpointing is enabled.",
    )
    parser.add_argument(
        "--scheduler",
        default="FIFO",
        type=str,
        help="FIFO (default), MedianStopping, AsyncHyperBand, "
        "HyperBand, or HyperOpt.",
    )
    parser.add_argument(
        "--scheduler-config",
        default="{}",
        type=json.loads,
        help="Config options to pass to the scheduler.",
    )

    # Note: this currently only makes sense when running a single trial
    parser.add_argument(
        "--restore",
        default=None,
        type=str,
        help="If specified, restore from this checkpoint.",
    )

    return parser


def _to_argv(config):
    """Converts configuration to a command line argument format."""
    argv = []
    for k, v in config.items():
        if "-" in k:
            raise ValueError("Use '_' instead of '-' in `{}`".format(k))
        if v is None:
            continue
        if not isinstance(v, bool) or v:  # for argparse flags
            argv.append("--{}".format(k.replace("_", "-")))
        if isinstance(v, str):
            argv.append(v)
        elif isinstance(v, bool):
            pass
        elif callable(v):
            argv.append(json.dumps(v, cls=TuneFunctionEncoder))
        else:
            argv.append(json.dumps(v, cls=SafeFallbackEncoder))
    return argv


_cached_pgf = {}


def _create_trial_from_spec(
    spec: dict, parser: argparse.ArgumentParser, **trial_kwargs
):
    """Creates a Trial object from parsing the spec.

    Args:
        spec: A resolved experiment specification. Arguments should
            The args here should correspond to the command line flags
            in ray.tune.experiment.config_parser.
        parser: An argument parser object from
            make_parser.
        trial_kwargs: Extra keyword arguments used in instantiating the Trial.

    Returns:
        A trial object with corresponding parameters to the specification.
    """
    global _cached_pgf

    spec = spec.copy()
    resources = spec.pop("resources_per_trial", None)

    try:
        args, _ = parser.parse_known_args(_to_argv(spec))
    except SystemExit:
        raise TuneError("Error parsing args, see above message", spec)

    if resources:
        trial_kwargs["placement_group_factory"] = resources

    checkpoint_config = spec.get("checkpoint_config", CheckpointConfig())

    return Trial(
        # Submitting trial via server in py2.7 creates Unicode, which does not
        # convert to string in a straightforward manner.
        trainable_name=spec["run"],
        # json.load leads to str -> unicode in py2.7
        config=spec.get("config", {}),
        # json.load leads to str -> unicode in py2.7
        stopping_criterion=spec.get("stop", {}),
        checkpoint_config=checkpoint_config,
        export_formats=spec.get("export_formats", []),
        # str(None) doesn't create None
        restore_path=spec.get("restore"),
        trial_name_creator=spec.get("trial_name_creator"),
        trial_dirname_creator=spec.get("trial_dirname_creator"),
        log_to_file=spec.get("log_to_file"),
        # str(None) doesn't create None
        max_failures=args.max_failures,
        storage=spec.get("storage"),
        **trial_kwargs,
    )
