"""BOHB (Bayesian Optimization with HyperBand)"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ConfigSpace
import ConfigSpace.hyperparameters as CSH
import ConfigSpace.util
import numpy as np
import scipy.stats as sps
import statsmodels.api as sm

from hpbandster.core.base_config_generator import base_config_generator
from ray.tune.suggest import SuggestionAlgorithm


class BOHB(base_config_generator):
    def __init__(self,
                 configspace,
                 min_points_in_model=None,
                 top_n_percent=15,
                 num_samples=64,
                 random_fraction=1 / 3,
                 bandwidth_factor=3,
                 min_bandwidth=1e-3,
                 **kwargs):
        """
            Fits for each given budget a kernel density estimator on the best N percent of the
            evaluated configurations on this budget.


            Parameters:
            -----------
            configspace: ConfigSpace
                Configuration space object
            top_n_percent: int
                Determines the percentile of configurations that will be used as training data
                for the kernel density estimator, e.g if set to 10 the 10% best configurations will be considered
                for training.
            min_points_in_model: int
                minimum number of datapoints needed to fit a model
            num_samples: int
                number of samples drawn to optimize EI via sampling
            random_fraction: float
                fraction of random configurations returned
            bandwidth_factor: float
                widens the bandwidth for contiuous parameters for proposed points to optimize EI
            min_bandwidth: float
                to keep diversity, even when all (good) samples have the same value for one of the parameters,
                a minimum bandwidth (Default: 1e-3) is used instead of zero.

        """
        super().__init__(**kwargs)
        self.top_n_percent = top_n_percent
        self.configspace = configspace
        self.bw_factor = bandwidth_factor
        self.min_bandwidth = min_bandwidth

        self.min_points_in_model = min_points_in_model
        if min_points_in_model is None:
            self.min_points_in_model = len(
                self.configspace.get_hyperparameters()) + 1

        if self.min_points_in_model < len(
                self.configspace.get_hyperparameters()) + 1:
            self.logger.warning(
                'Invalid min_points_in_model value. Setting it to %i' %
                (len(self.configspace.get_hyperparameters()) + 1))
            self.min_points_in_model = len(
                self.configspace.get_hyperparameters()) + 1

        self.num_samples = num_samples
        self.random_fraction = random_fraction

        hps = self.configspace.get_hyperparameters()

        self.kde_vartypes = ""
        self.vartypes = []

        for h in hps:
            if hasattr(h, 'sequence'):
                raise RuntimeError(
                    'This version on BOHB does not support ordinal hyperparameters. Please encode %s as an integer parameter!'
                    % (h.name))

            if hasattr(h, 'choices'):
                self.kde_vartypes += 'u'
                self.vartypes += [len(h.choices)]
            else:
                self.kde_vartypes += 'c'
                self.vartypes += [0]

        self.vartypes = np.array(self.vartypes, dtype=int)

        # store precomputed probs for the categorical parameters
        self.cat_probs = []

        self.configs = dict()
        self.losses = dict()
        self.good_config_rankings = dict()
        self.kde_models = dict()

    def largest_budget_with_model(self):
        if len(self.kde_models) == 0:
            return (-float('inf'))
        return (max(self.kde_models.keys()))

    def get_config(self, *args):
        """
            Function to sample a new configuration

            This function is called inside Hyperband to query a new configuration


            returns: config
                should return a valid configuration

        """

        self.logger.debug('start sampling a new configuration.')

        sample = None
        info_dict = {}

        # If no model is available, sample from prior
        # also mix in a fraction of random configs
        if len(self.kde_models.keys()
               ) == 0 or np.random.rand() < self.random_fraction:
            sample = self.configspace.sample_configuration()
            info_dict['model_based_pick'] = False

        best = np.inf
        best_vector = None

        if sample is None:
            try:

                #sample from largest budget
                budget = max(self.kde_models.keys())

                l = self.kde_models[budget]['good'].pdf
                g = self.kde_models[budget]['bad'].pdf

                minimize_me = lambda x: max(1e-32, g(x)) / max(l(x), 1e-32)

                kde_good = self.kde_models[budget]['good']
                kde_bad = self.kde_models[budget]['bad']

                for i in range(self.num_samples):
                    idx = np.random.randint(0, len(kde_good.data))
                    datum = kde_good.data[idx]
                    vector = []

                    for m, bw, t in zip(datum, kde_good.bw, self.vartypes):

                        bw = max(bw, self.min_bandwidth)
                        if t == 0:
                            bw = self.bw_factor * bw
                            try:
                                vector.append(
                                    sps.truncnorm.rvs(
                                        -m / bw, (1 - m) / bw, loc=m,
                                        scale=bw))
                            except:
                                self.logger.warning(
                                    "Truncated Normal failed for:\ndatum=%s\nbandwidth=%s\nfor entry with value %s"
                                    % (datum, kde_good.bw, m))
                                self.logger.warning(
                                    "data in the KDE:\n%s" % kde_good.data)
                        else:

                            if np.random.rand() < (1 - bw):
                                vector.append(int(m))
                            else:
                                vector.append(np.random.randint(t))
                    val = minimize_me(vector)

                    if not np.isfinite(val):
                        self.logger.warning(
                            'sampled vector: %s has EI value %s' % (vector,
                                                                    val))
                        self.logger.warning("data in the KDEs:\n%s\n%s" %
                                            (kde_good.data, kde_bad.data))
                        self.logger.warning("bandwidth of the KDEs:\n%s\n%s" %
                                            (kde_good.bw, kde_bad.bw))
                        self.logger.warning("l(x) = %s" % (l(vector)))
                        self.logger.warning("g(x) = %s" % (g(vector)))

                        # right now, this happens because a KDE does not contain all values for a categorical parameter
                        # this cannot be fixed with the statsmodels KDE, so for now, we are just going to evaluate this one
                        # if the good_kde has a finite value, i.e. there is no config with that value in the bad kde, so it shouldn't be terrible.
                        if np.isfinite(l(vector)):
                            best_vector = vector
                            break

                    if val < best:
                        best = val
                        best_vector = vector

                if best_vector is None:
                    self.logger.debug(
                        "Sampling based optimization with %i samples failed -> using random configuration"
                        % self.num_samples)
                    sample = self.configspace.sample_configuration(
                    ).get_dictionary()
                    info_dict['model_based_pick'] = False
                else:
                    self.logger.debug('best_vector: {}, {}, {}, {}'.format(
                        best_vector, best, l(best_vector), g(best_vector)))
                    for i, hp_value in enumerate(best_vector):
                        if isinstance(
                                self.configspace.get_hyperparameter(
                                    self.configspace.get_hyperparameter_by_idx(
                                        i)), CSH.CategoricalHyperparameter):
                            best_vector[i] = int(np.rint(best_vector[i]))
                    sample = ConfigSpace.Configuration(
                        self.configspace, vector=best_vector).get_dictionary()

                    try:
                        sample = ConfigSpace.util.deactivate_inactive_hyperparameters(
                            configuration_space=self.configspace,
                            configuration=sample)
                        info_dict['model_based_pick'] = True

                    except Exception as e:
                        self.logger.warning(("="*50 + "\n")*3 +\
                          "Error converting configuration:\n%s"%sample+\
                          "\n here is a traceback:" +\
                          traceback.format_exc())
                        raise (e)

            except:
                self.logger.warning(
                    "Sampling based optimization with %i samples failed\n %s \nUsing random configuration"
                    % (self.num_samples, traceback.format_exc()))
                sample = self.configspace.sample_configuration()
                info_dict['model_based_pick'] = False

        try:
            sample = ConfigSpace.util.deactivate_inactive_hyperparameters(
                configuration_space=self.configspace,
                configuration=sample.get_dictionary()).get_dictionary()
        except Exception as e:
            self.logger.warning(
                "Error (%s) converting configuration: %s -> "
                "using random configuration!", e, sample)
            sample = self.configspace.sample_configuration().get_dictionary()
        self.logger.debug('done sampling a new configuration.')
        return sample, info_dict

    def impute_conditional_data(self, array):

        return_array = np.empty_like(array)

        for i in range(array.shape[0]):
            datum = np.copy(array[i])
            nan_indices = np.argwhere(np.isnan(datum)).flatten()

            while (np.any(nan_indices)):
                nan_idx = nan_indices[0]
                valid_indices = np.argwhere(np.isfinite(
                    array[:, nan_idx])).flatten()

                if len(valid_indices) > 0:
                    # pick one of them at random and overwrite all NaN values
                    row_idx = np.random.choice(valid_indices)
                    datum[nan_indices] = array[row_idx, nan_indices]

                else:
                    # no good point in the data has this value activated, so fill it with a valid but random value
                    t = self.vartypes[nan_idx]
                    if t == 0:
                        datum[nan_idx] = np.random.rand()
                    else:
                        datum[nan_idx] = np.random.randint(t)

                nan_indices = np.argwhere(np.isnan(datum)).flatten()
            return_array[i, :] = datum
        return (return_array)

    def new_result(self, job, update_model=True):
        """
            function to register finished runs

            Every time a run has finished, this function should be called
            to register it with the result logger. If overwritten, make
            sure to call this method from the base class to ensure proper
            logging.


            Parameters:
            -----------
            job: hpbandster.distributed.dispatcher.Job object
                contains all the info about the run
        """

        super().new_result(job)

        if job.result is None:
            # One could skip crashed results, but we decided to
            # assign a +inf loss and count them as bad configurations
            loss = np.inf
        else:
            # same for non numeric losses.
            # Note that this means losses of minus infinity will count as bad!
            loss = job.result["loss"] if np.isfinite(
                job.result["loss"]) else np.inf

        budget = job.kwargs["budget"]

        if budget not in self.configs.keys():
            self.configs[budget] = []
            self.losses[budget] = []

        # skip model building if we already have a bigger model
        if max(list(self.kde_models.keys()) + [-np.inf]) > budget:
            return

        # We want to get a numerical representation of the configuration in the original space

        conf = ConfigSpace.Configuration(self.configspace,
                                         job.kwargs["config"])
        self.configs[budget].append(conf.get_array())
        self.losses[budget].append(loss)

        # skip model building:
        #       a) if not enough points are available
        if len(self.configs[budget]) <= self.min_points_in_model - 1:
            self.logger.debug(
                "Only %i run(s) for budget %f available, need more than %s -> can't build model!"
                % (len(self.configs[budget]), budget,
                   self.min_points_in_model + 1))
            return

        #       b) during warnm starting when we feed previous results in and only update once
        if not update_model:
            return

        train_configs = np.array(self.configs[budget])
        train_losses = np.array(self.losses[budget])

        n_good = max(self.min_points_in_model,
                     (self.top_n_percent * train_configs.shape[0]) // 100)
        #n_bad = min(max(self.min_points_in_model, ((100-self.top_n_percent)*train_configs.shape[0])//100), 10)
        n_bad = max(
            self.min_points_in_model,
            ((100 - self.top_n_percent) * train_configs.shape[0]) // 100)

        # Refit KDE for the current budget
        idx = np.argsort(train_losses)

        train_data_good = self.impute_conditional_data(
            train_configs[idx[:n_good]])
        train_data_bad = self.impute_conditional_data(
            train_configs[idx[n_good:n_good + n_bad]])

        if train_data_good.shape[0] <= train_data_good.shape[1]:
            return
        if train_data_bad.shape[0] <= train_data_bad.shape[1]:
            return

        #more expensive crossvalidation method
        #bw_estimation = 'cv_ls'

        # quick rule of thumb
        bw_estimation = 'normal_reference'

        bad_kde = sm.nonparametric.KDEMultivariate(
            data=train_data_bad, var_type=self.kde_vartypes, bw=bw_estimation)
        good_kde = sm.nonparametric.KDEMultivariate(
            data=train_data_good, var_type=self.kde_vartypes, bw=bw_estimation)

        bad_kde.bw = np.clip(bad_kde.bw, self.min_bandwidth, None)
        good_kde.bw = np.clip(good_kde.bw, self.min_bandwidth, None)

        self.kde_models[budget] = {'good': good_kde, 'bad': bad_kde}

        # update probs for the categorical parameters for later sampling
        self.logger.debug(
            'done building a new model for budget %f based on %i/%i split\nBest loss for this budget:%f\n\n\n\n\n'
            % (budget, n_good, n_bad, np.min(train_losses)))


class JobWrapper():
    """Dummy class"""

    def __init__(self, loss, budget, config):
        self.result = {"loss": loss}
        self.kwargs = {"budget": budget, "config": config.copy()}
        self.exception = None

class TuneBOHB(SuggestionAlgorithm):
    """Tune Suggestion Algorithm for BOHB code"""

    def __init__(self,
                 space,
                 max_concurrent=8,
                 metric="neg_mean_loss",
                 bohb_config=None):
        self._max_concurrent = max_concurrent
        self.trial_to_params = {}
        self.running = set()
        self.paused = set()
        self.metric = metric
        bohb_config = bohb_config or {}
        self.bohber = BOHB(space, **bohb_config)
        super(TuneBOHB, self).__init__()

    def _suggest(self, trial_id):
        if len(self.running) < self._max_concurrent:
            config, info = self.bohber.get_config()
            self.trial_to_params[trial_id] = list(config)
            self.running.add(trial_id)
            return config
        return None

    def on_trial_result(self, trial_id, result):
        if trial_id not in self.paused:
            self.running.add(trial_id)
        if "budget" in result.get("hyperband_info", {}):
            hbs_wrapper = self.to_wrapper(trial_id, result)
            print("adding new result", vars(hbs_wrapper))
            self.bohber.new_result(hbs_wrapper)

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        del self.trial_to_params[trial_id]
        if trial_id in self.paused:
            self.paused.remove(trial_id)
        elif trial_id in self.running:
            self.running.remove(trial_id)
        else:
            import ipdb; ipdb.set_trace()


    def to_wrapper(self, trial_id, result):
        return JobWrapper(
            -result[self.metric], result["hyperband_info"]["budget"],
            {k: result["config"][k] for k in self.trial_to_params[trial_id]})

    def on_pause(self, trial_id):
        self.paused.add(trial_id)
        self.running.remove(trial_id)

    def on_unpause(self, trial_id):
        self.paused.remove(trial_id)
        self.running.add(trial_id)