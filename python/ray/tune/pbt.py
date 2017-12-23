from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np
import random
import math
import copy

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial

import ray

class PopulationBasedTraining(FIFOScheduler):
    """Implements the Population Based Training algorithm as described in the PBT paper:

    Args:
        time_attr (str): The TrainingResult attr to use for documenting length
            of time since last ready() call. Attribute only has to increase
            monotonically
        reward_attr (str): The TrainingResult objective value attribute. As with
            'time_attr'. this may refer to any objective value that is supposed
            to increase with time.
        grace_period (float): Period of time, in which algorithm will not compare 
            model to other models
        perturbation_interval (float): Used in the default ready function to determine
            if enough time has passed so that a agent can be tested for readiness.
        ready (func): Determines in Trial is ready to undergo the comparison,
            exploitation, and exploration process. Should return boolean
        hyperparamter_mutations (dict); Possible values that each hyperparameter can mutate
            to, as certain hyperparameters only work with certain values
    """
    
    def __init__(
            self, time_attr="training_iteration", reward_attr='episode_reward_mean',
            grace_period=10.0, perturbation_interval=6.0, ready=None, hyperparameter_mutations=None):
        FIFOScheduler.__init__(self)
        self._stopped_trials = set()
        self._completed_trials = set()
        self._results = collections.defaultdict(list)
        self._last_perturbation_time = {}
        self._grace_period = grace_period
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        self._ready_func = ready
        self._hyperparameter_mutations = hyperparameter_mutations
        self._perturbation_interval = perturbation_interval
        self._checkpoint_paths = {}
        
    def on_trial_result(self, trial_runner, trial, result):

        print("wowee, trial had a result")
        print(getattr(trial, "config"))
        assert trial.checkpoint_freq != None, "Trials need a checkpoint frequency"
        self._results[trial].append(result)
        time = getattr(result, self._time_attr)
        ready = False
        #check model is ready to undergo mutation, based on user function or default
        #function
        if time > self._grace_period:
            ready = self._ready_func(result, trial) if self._ready_func is not None else self._default_ready(result, trial, time) and self._truncation_ready(result, trial, time)
        else:
            print("grace period")
            ready = False
        if ready:
            print("ready to undergo mutation")
            print ("----")
            print("Current Trial is: {0}".format(trial))
            #get best trial for current time
            best_trial = self._get_best_trial(result, time)
            print("Best Trial is: {0}".format(best_trial))
            print(best_trial.config)
            
            #if current trial is the best trial (as in same hyperparameters), do nothing
            if trial.config == best_trial.config:
                print("current trial is best trial")
                return TrialScheduler.CONTINUE
            else:
                #setup the trial runner in case it
                #create a new trial, with the same weights and hyperparamters as the best_trial
                #if trial has finished/paused, restart the runner, so that checkpoint works
                # best_trial_was_stopped = False
                # if best_trial.status == "TERMINATED" or best_trial.status == "PAUSED":
                #     best_trial_was_stopped = True
                #     best_trial._setup_runner()
                self._exploit(self._hyperparameter_mutations, best_trial, trial, trial_runner, time)

                return TrialScheduler.STOP
                


        else:
            print("not ready")
            # self._checkpoint_paths[trial] = trial.checkpoint()
        
        return TrialScheduler.CONTINUE

    def on_trial_complete(self, trial_runner, trial, result):
        self._results[trial].append(result)
        self._completed_trials.add(trial)
        # self._checkpoint_paths[trial] = trial.checkpoint()
        print("finished a trial")
        min_time = 0
        best_trial = None
        for trial in self._completed_trials:
            last_result = self._results[trial][-1]
            if getattr(last_result, self._time_attr) < min_time or min_time == 0:
                min_time = getattr(last_result, self._time_attr)
                best_trial = trial
        
        print("The Best Trial is currently {0} finishing in {1} iterations, with the hyperparameters of {2}".format(best_trial, min_time, best_trial.config))

    def _exploit(self, hyperparameter_mutations, best_trial, trial, trial_runner, time):
        
        mutate_string = "_mutated@" + str(time)
        hyperparams = copy.deepcopy(best_trial.config)
        hyperparams = self._explore(hyperparams, hyperparameter_mutations, best_trial)
        print("new hyperparamter configuration: {0}".format(hyperparams))
        # checkpoint = self._checkpoint_paths[best_trial]
        checkpoint = best_trial._checkpoint_path
        new_trial = Trial(
            trainable_name = best_trial.trainable_name,
            config = hyperparams,
            local_dir= best_trial.local_dir,
            experiment_tag= best_trial.experiment_tag + mutate_string,
            resources = best_trial.resources,
            stopping_criterion= best_trial.stopping_criterion,
            checkpoint_freq= best_trial.checkpoint_freq,
            restore_path= checkpoint,
            upload_dir= best_trial.upload_dir
        )
        
        #mutate the hyperparamters the new trial, last index of config is always the "env"
        #so exclude that, temporarily we "perturb" each hyperparameter by a factor 1.2 or 0.8 and then round it to the nearest
        #even integer
        print(new_trial.config)
        new_trial.experiment_tag = trial.experiment_tag + "-mutated@" + str(time)
        #add the new trial to the trialrunner, and then stop the
        #trial it replaced
        trial_runner._launch_trial(new_trial)
        trial_runner.add_trial(new_trial)
        self._results[new_trial] = self._results[trial]
        self._last_perturbation_time[new_trial] = self._last_perturbation_time[trial]
        self._results.pop(trial)
        self._last_perturbation_time.pop(trial)



        

    def _explore(self, hyperparams, hyperparameter_mutations, best_trial):

        if hyperparameter_mutations != None:
            hyperparams = {param: random.choice(hyperparameter_mutations[param]) for param in hyperparams if param != "env" and param in hyperparameter_mutations}
            for param in best_trial.config:
                if param not in hyperparameter_mutations and param != "env":
                    hyperparams[param] = self._round_up_to_even(best_trial.config[param] * random.choice([0.8, 1.2]))
        else:
            hyperparams = {param: self._round_up_to_even(random.choice([0.8, 1.2]) * hyperparams[param]) for param in hyperparams if param != "env"}
        hyperparams["env"] = best_trial.config["env"]
        return hyperparams

    def _default_ready(self, result, trial, time):
        #function checks if trial has been added to the time tracker, if not adds
        #it, else checks if an appropriate amount of time has passed since the last mutation
        if trial not in self._last_perturbation_time:
            print("added trial to time tracker")
            self._last_perturbation_time[trial] = (time)
        else:
            time_since_last = time - self._last_perturbation_time[trial]
            if time_since_last >= self._perturbation_interval:
                self._last_perturbation_time[trial] = time
                return True
        return False

    def _truncation_ready(self, result, trial, time):
        #function checks if trial is in the bottom 20% of all trials, and if so, is ready
        sorted_result_keys = sorted(self._results, key=lambda x: max(self._results.get(x) if self._results.get(x) else [0]))
        max_index = int(round(len(sorted_result_keys) * 0.2))
        for i in range(0, max_index):
            if trial == sorted_result_keys[i]:
                print("{0} is in the bottomn 20 percent of {1}, truncation is ready".format(trial, [x.experiment_tag for x in sorted_result_keys]))
                return True
        print("truncation not ready")
        return False
        

    def _get_best_trial(self, result, time):
        results_at_time = {}
        for trial in self._results:
            results_at_time[trial] = [getattr(r, self._reward_attr)
                for r in self._results[trial] if getattr(r, self._time_attr) <= time]
        print("Results at {0}: {1}".format(time, results_at_time))
        return max(results_at_time, key=lambda x: max(results_at_time.get(x) if results_at_time.get(x) else [0]))

    def _round_up_to_even(self, n):
        return math.ceil(n/2.) * 2

    def _is_empty(self, x):
        if x:
            return False
        return True

    def debug_string(self):
        return "Using PBT"
    