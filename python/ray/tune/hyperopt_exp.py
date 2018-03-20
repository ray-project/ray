from hyperopt import FMinIter
from hyperopt import base, utils
from ray.tune.config_parser import make_parser, json_to_resources, resources_to_json
from ray.tune.variant_generator import to_argv
import time
import copy
import numpy as np
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.trial_runner import TrialRunner
from ray.tune import TuneError
from ray.tune import register_trainable
from ray.tune import Trainable
from ray.tune.result import TrainingResult
from ray.tune.trial_scheduler import TrialScheduler

from ray.tune.trial_scheduler import FIFOScheduler
from hyperopt import tpe, Domain, Trials


# class MyTrainableClass(Trainable):
#     """Example agent whose learning curve is a random sigmoid.

#     The dummy hyperparameters "width" and "height" determine the slope and
#     maximum reward value reached.
#     """

#     def _setup(self):
#         self.timestep = 0

#     def _train(self):
#         self.timestep += 1
#         v = np.tanh(float(self.timestep) / self.config["width"])
#         v *= self.config["height"]

#         # Here we use `episode_reward_mean`, but you can also report other
#         # objectives such as loss or accuracy (see tune/result.py).
#         return TrainingResult(episode_reward_mean=v, timesteps_this_iter=1)

#     def _save(self, checkpoint_dir):
#         path = os.path.join(checkpoint_dir, "checkpoint")
#         with open(path, "w") as f:
#             f.write(json.dumps({"timestep": self.timestep}))
#         return path

#     def _restore(self, checkpoint_path):
#         with open(checkpoint_path) as f:
#             self.timestep = json.loads(f.read())["timestep"]



def easy_objective(config, reporter):

    # val = config["height"]
    time.sleep(0.5)
    reporter(
        timesteps_total=1,
        mean_loss=((config["height"] - 14) ** 2 + abs(config["width"] - 3)))
    time.sleep(0.5)


class HyperOptScheduler(FIFOScheduler):

    def __init__(self, experiments, max_concurrent=10, loss_attr="mean_loss"):
        assert len(experiments) == 1, "Currently only support 1 experiment"
        name, _spec = list(experiments.keys())[0], list(experiments.values())[0]

        spec = copy.deepcopy(_spec)
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        space = spec["config"]["space"]
        del  spec["config"]["space"]

        self.parser = make_parser()
        self.args = self.parser.parse_args(to_argv(spec))
        self.default_config = copy.deepcopy(spec["config"])

        self.algo = tpe.suggest
        self.domain = Domain(lambda spec: spec, space)
        self._hpopt_trials = Trials()
        self._tune_to_hp = {}
        self._loss_attr = loss_attr
        self._num_trials_left = self.args.repeat

        self.max_concurrent = min(max_concurrent, self._num_trials_left)
        self.rstate = np.random.RandomState()

    def generate_trial(self):
        if self._num_trials_left <= 0:
            raise TuneError("No trials left!")
        new_cfg = copy.deepcopy(self.default_config)
        new_ids = self._hpopt_trials.new_trial_ids(1)
        self._hpopt_trials.refresh()

        new_trials = self.algo(new_ids, self.domain, self._hpopt_trials,
                          self.rstate.randint(2 ** 31 - 1))

        self._hpopt_trials.insert_trial_docs(new_trials)
        self._hpopt_trials.refresh()
        new_trial = new_trials[0]
        new_trial_id = new_trial["tid"]

        # TODO(rliaw): get suggested config
        suggested_config = base.spec_from_misc(new_trial['misc'])
        new_cfg.update(suggested_config)
        kv_str = "_".join(
            ["{}={}".format(k, str(v)[:5]) for k, v in suggested_config.items()])
        experiment_tag = "hyperopt_{}_{}".format(
            new_trial_id, kv_str)

        trial = Trial(
            trainable_name=self.args.run,
            config=new_cfg,
            local_dir=self.args.local_dir,
            experiment_tag=experiment_tag,
            resources=self.args.resources,
            stopping_criterion=self.args.stop,
            checkpoint_freq=self.args.checkpoint_freq,
            restore_path=self.args.restore,
            upload_dir=self.args.upload_dir)
        self._tune_to_hp[trial] = new_trial_id
        self._num_trials_left -= 1
        print("Adding new trial - {}".format(len(self._tune_to_hp)))

        return trial

    def on_trial_result(self, trial_runner, trial, result):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        now = utils.coarse_utcnow()
        ho_trial['book_time'] = now
        ho_trial['refresh_time'] = now
        return TrialScheduler.CONTINUE

    def on_trial_error(self, trial_runner, trial):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['state'] = base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Error")
        ho_trial['refresh_time'] = utils.coarse_utcnow()
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_complete(self, trial_runner, trial, result):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['state'] = base.JOB_STATE_DONE
        hp_result = self._convert_result(result)
        ho_trial['result'] = hp_result
        ho_trial['refresh_time'] = utils.coarse_utcnow()
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]
        if self._continue():
            trial_runner.add_trial(self.generate_trial())

    def _convert_result(self, result):
        return {"loss": getattr(result, self._loss_attr),
                "status": "ok"}

    def _get_dynamic_trial(self, tid):
        return [t for t in self._hpopt_trials.trials if t["tid"] == tid][0]

    def _continue(self):
        return self._num_trials_left > 0

    def get_hyperopt_trials(self):
        return self._hpopt_trials



def run_experiments(experiments, with_server=False,
                    server_port=4321, verbose=True):

    # Make sure rllib agents are registered
    from ray import rllib  # noqa # pylint: disable=unused-import

    scheduler = HyperOptScheduler(experiments, max_concurrent=8)
    runner = TrialRunner(
        scheduler, launch_web_server=with_server, server_port=server_port)

    # TODO(rliaw): bound this by max number of generated trials
    for i in range(scheduler.max_concurrent):  # number of concurrent trials
        trial = scheduler.generate_trial()
        trial.set_verbose(verbose)
        runner.add_trial(trial)
    print(runner.debug_string(max_debug=99999))

    last_debug = 0
    while not runner.is_finished():
        runner.step()
        if time.time() - last_debug > DEBUG_PRINT_INTERVAL:
            print(runner.debug_string())
            last_debug = time.time()

    print(runner.debug_string(max_debug=99999))

    for trial in runner.get_trials():
        # TODO(rliaw): What about errored?
        if trial.status != Trial.TERMINATED:
            raise TuneError("Trial did not complete", trial)

    import ipdb; ipdb.set_trace()
    t = scheduler.get_hyperopt_trials()


    import matplotlib.pyplot as plt
    from scipy.signal import convolve

    loss = [x['result']['loss'] for x in t.trials]
    filt = np.ones(5) / 5
    plt.plot(convolve(loss, filt))
    plt.show()

    return runner.get_trials()


if __name__ == '__main__':
    import ray
    ray.init(redirect_output=True)
    from hyperopt import hp
    # register_trainable("exp", MyTrainableClass)

    register_trainable("exp", easy_objective)

    space = {
        'width': hp.uniform('width', 0, 20),
        'height': hp.uniform('height', -100, 100),
    }

    config = {"my_exp": {
            "run": "exp",
            "repeat": 200,
            "stop": {"training_iteration": 1},
            "config": {
                "space": space}}}
    run_experiments(config, verbose=False)
