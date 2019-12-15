"""
scikit-learn compatible wrapper for ray.tune.

Based on code from scikit-learn.
"""
import logging
import os
import pickle
import time
from abc import abstractmethod, ABC
from copy import deepcopy
from functools import partial

import numpy as np
import pandas as pd

import ray.tune
from ray import tune

logger = logging.getLogger(__name__)

try:
    import sklearn
except ImportError:
    sklearn = None
    logger.exception("Couldn't import `sklearn` library. "
                     "Install it for scikit-learn SearchCV API.")

if sklearn is not None:
    sklearn_version = tuple(int(v) for v in sklearn.__version__.split(".")[:2])

    minimal_sklearn_version = (0, 21)
    if not sklearn_version >= minimal_sklearn_version:
        logger.warning("Using outdated scikit-learn version. "
                       "Install version >= %d.%d." % minimal_sklearn_version)

from ._sklearn.metrics._scorer import _check_multimetric_scoring  # noqa
from ._sklearn.model_selection._validation import _fit_and_score, \
                                                   _aggregate_score_dicts  # noqa

from sklearn.utils.metaestimators import if_delegate_has_method  # noqa


class BaseSearchCV(sklearn.base.BaseEstimator, sklearn.base.MetaEstimatorMixin,
                   ABC):
    """Abstract base class for hyper parameter search with cross-validation.
    """

    def __init__(
            self,
            *,
            estimator,
            scoring=None,
            refit=True,
            cv=5,
            verbose=0,
            error_score=np.nan,
            return_train_score=False,
            resources_per_trial=None,
    ):
        self.scoring = scoring
        self.estimator = estimator
        self.refit = refit
        self.cv = cv
        self.verbose = verbose
        self.error_score = error_score
        self.return_train_score = return_train_score
        self.tune_params = {}  # default empty dict
        self.resources_per_trial = ({
            "cpu": 1
        } if resources_per_trial is None else resources_per_trial)

    def _check_is_fitted(self, method_name):
        if not self.refit:
            raise sklearn.exceptions.NotFittedError(
                "This %s instance was initialized "
                "with refit=False. %s is "
                "available only after refitting on the best "
                "parameters. You can refit an estimator "
                "manually using the ``best_params_`` "
                "attribute" % (type(self).__name__, method_name))
        else:
            sklearn.utils.validation.check_is_fitted(self, "best_estimator_")

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def predict(self, X):
        """Call predict on the estimator with the best found parameters.

        Only available if ``refit=True`` and the underlying estimator supports
        ``predict``.

        Parameters
        ----------
        X : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("predict")
        return self.best_estimator_.predict(X)

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def predict_proba(self, X):
        """Call predict_proba on the estimator with the best found parameters.

        Only available if ``refit=True`` and the underlying estimator supports
        ``predict_proba``.

        Parameters
        ----------
        X : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("predict_proba")
        return self.best_estimator_.predict_proba(X)

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def predict_log_proba(self, X):
        """Call predict_log_proba on the estimator with the best found parameters.

        Only available if ``refit=True`` and the underlying estimator supports
        ``predict_log_proba``.

        Parameters
        ----------
        X : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("predict_log_proba")
        return self.best_estimator_.predict_log_proba(X)

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def decision_function(self, X):
        """Call decision_function on the estimator with the best found parameters.

        Only available if ``refit=True`` and the underlying estimator supports
        ``decision_function``.

        Parameters
        ----------
        X : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("decision_function")
        return self.best_estimator_.decision_function(X)

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def transform(self, X):
        """Call transform on the estimator with the best found parameters.

        Only available if the underlying estimator supports ``transform`` and
        ``refit=True``.

        Parameters
        ----------
        X : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("transform")
        return self.best_estimator_.transform(X)

    @if_delegate_has_method(delegate=("best_estimator_", "estimator"))
    def inverse_transform(self, Xt):
        """Call inverse_transform on the estimator with the best found params.

        Only available if the underlying estimator implements
        ``inverse_transform`` and ``refit=True``.

        Parameters
        ----------
        Xt : indexable, length n_samples
            Must fulfill the input assumptions of the
            underlying estimator.

        """
        self._check_is_fitted("inverse_transform")
        return self.best_estimator_.inverse_transform(Xt)

    @property
    def classes_(self):
        self._check_is_fitted("classes_")
        return self.best_estimator_.classes_

    def fit(self, X, y=None, groups=None, tune_params=None, **fit_params):
        """Run fit with all sets of parameters. Fit is performed with Ray Tune.

        Parameters
        ----------

        X : array-like, shape = [n_samples, n_features]
            Training vector, where n_samples is the number of samples and
            n_features is the number of features.

        y : array-like, shape = [n_samples] or [n_samples, n_output], optional
            Target relative to X for classification or regression;
            None for unsupervised learning.

        groups : array-like, with shape (n_samples,), optional
            Group labels for the samples used while splitting the dataset into
            train/test set. Only used in conjunction with a "Group" `cv`
            instance (e.g., `GroupKFold`).

        tune_params : dict of string -> object
            Parameters passed to the ``ray.tune.run``.

        **fit_params : dict of string -> object
            Parameters passed to the ``fit`` method of the estimator.
        """
        cv = sklearn.model_selection.check_cv(
            self.cv, y, classifier=sklearn.base.is_classifier)

        scorers, self.multimetric_ = _check_multimetric_scoring(
            self.estimator, scoring=self.scoring)

        if self.multimetric_:
            if (self.refit is not False
                    and (not isinstance(self.refit, str) or
                         # This will work for both dict / list (tuple)
                         self.refit not in scorers)
                    and not callable(self.refit)):
                raise ValueError(
                    "For multi-metric scoring, the parameter "
                    "refit must be set to a scorer key "
                    "to refit an estimator with the "
                    "best parameter setting on the whole "
                    "data and make the best_* attributes "
                    "available for that metric. If this is "
                    "not needed, refit should be set to "
                    "False explicitly. %r was passed." % self.refit)
            else:
                refit_metric = self.refit
        else:
            refit_metric = "score"

        X, y, groups = sklearn.utils.indexable(X, y, groups)
        n_splits = cv.get_n_splits(X, y, groups)

        base_estimator = sklearn.clone(self.estimator)

        fit_and_score_kwargs = dict(
            scorer=scorers,
            fit_params=fit_params,
            return_train_score=self.return_train_score,
            return_n_test_samples=True,
            return_times=True,
            return_parameters=False,
            error_score=self.error_score,
            verbose=self.verbose,
        )

        all_tune_params = self._get_search_space_config().copy()
        all_tune_params.update(self.tune_params)

        if tune_params is not None:
            all_tune_params.update(tune_params)

        analysis = ray.tune.run(
            partial(
                BaseSearchCV._fit_and_score,
                self,
                X=X,
                y=y,
                cv=cv,
                groups=groups,
                n_splits=n_splits,
                fit_and_score_kwargs=fit_and_score_kwargs,
                scorers=scorers,
            ),
            **all_tune_params,
        )

        # For multi-metric evaluation, store the best_index_, best_params_ and
        # best_score_ iff refit is one of the scorer names
        # In single metric evaluation, refit_metric is "score"
        if self.refit or not self.multimetric_:
            # If callable, refit is expected to return the index of the best
            # parameter set.
            if callable(self.refit):
                raise TypeError  # callable is not supported

            metric_column = "mean_test_%s" % refit_metric
            analysis_dataframe = analysis.dataframe(
                metric=metric_column, mode="max")
            best_experiment = analysis_dataframe.iloc[analysis_dataframe[
                metric_column].idxmax()]

            self.best_score_ = best_experiment[metric_column]
            self.best_params_ = analysis.get_all_configs()[best_experiment[
                "logdir"]]
        else:
            best_experiment = None

        if self.refit:
            best_estimator_ = sklearn.clone(base_estimator).set_params(
                **self.best_params_)

            if y is not None:
                fit_args = [X, y]
            else:
                fit_args = [X]

            best_estimator_object_id = self._make_function_remote(
                _fit_estimator).remote(best_estimator_, *fit_args,
                                       **fit_params)

            self.refit_time_, self.best_estimator_ = ray.get(
                best_estimator_object_id)

        # Store the only scorer not as a dict for single metric evaluation
        self.scorer_ = scorers if self.multimetric_ else scorers["score"]

        self.trial_dataframes_ = analysis.fetch_trial_dataframes()
        self.n_splits_ = n_splits

        # cv_results_ is loose imitation of ``cv_results_`` from sklearn
        trial_dataframes = deepcopy(self.trial_dataframes_)
        for k, v in trial_dataframes.items():
            trial_dataframes[k]["logdir"] = k

        trial_dataframes = pd.concat(
            trial_dataframes.values(), ignore_index=True)

        self.cv_results_ = trial_dataframes.to_dict("list")
        self.cv_results_["params"] = [
            analysis.get_all_configs()[logdir]
            for logdir in self.cv_results_["logdir"]
        ]

        if best_experiment is not None:
            self.best_index_ = self.cv_results_["logdir"].index(
                best_experiment["logdir"])

        for k, v in self.cv_results_.items():
            self.cv_results_[k] = np.array(v)

        return self

    def _make_function_remote(self, f):
        remote_resources = self.resources_per_trial
        return ray.remote(
            num_cpus=remote_resources.pop("cpu", 1),
            num_gpus=remote_resources.pop("gpu", 0),
            resources=remote_resources,
        )(f)

    def _fit_and_score(
            self,
            config,
            reporter,
            X,
            y,
            cv,
            groups,
            n_splits,
            fit_and_score_kwargs,
            scorers,
    ):
        """
        Tune wrapper over sklearn `_fit_and_score` and `_format_results`.

        Expected to run inside process with `self.resources_per_trial`
        resources.
        """

        out = []
        for i, (train, test) in enumerate(cv.split(X, y, groups)):
            estimator = sklearn.clone(self.estimator)

            out.append(
                _fit_and_score(
                    estimator,
                    X,
                    y,
                    train=train,
                    test=test,
                    parameters=config,
                    **fit_and_score_kwargs,
                ))

            with open(
                    os.path.join(reporter.logdir, "model_split_%d.pkl" % i),
                    "wb") as fp:
                pickle.dump(estimator, fp)

        if len(out) < 1:
            raise ValueError("No fits were performed. "
                             "Was the CV iterator empty? "
                             "Were there no candidates?")
        elif len(out) != n_splits:
            raise ValueError("cv.split and cv.get_n_splits returned "
                             "inconsistent results. Expected {} "
                             "splits, got {}".format(n_splits, len(out)))

        result = {}

        # if one choose to see train score, "out" will contain train score info
        if self.return_train_score:
            (
                train_score_dicts,
                test_score_dicts,
                test_sample_counts,
                fit_time,
                score_time,
            ) = zip(*out)
        else:
            (test_score_dicts, test_sample_counts, fit_time,
             score_time) = zip(*out)

        test_scores = _aggregate_score_dicts(test_score_dicts)
        if self.return_train_score:
            train_scores = _aggregate_score_dicts(train_score_dicts)

        def _store(key_name, array, splits=False):
            """A small helper to store the scores/times"""
            array = np.array(array, dtype=np.float64)
            if splits:
                for split_i in range(n_splits):
                    result["split%d_%s" % (split_i, key_name)] = array[split_i]

            result["mean_%s" % key_name] = np.mean(array)
            result["std_%s" % key_name] = np.std(array)

        _store("fit_time", fit_time)
        _store("score_time", score_time)

        for scorer_name in scorers.keys():
            # Computed the (weighted) mean and std for test scores alone
            _store(
                "test_%s" % scorer_name, test_scores[scorer_name], splits=True)
            if self.return_train_score:
                _store(
                    "train_%s" % scorer_name,
                    train_scores[scorer_name],
                    splits=True)

        reporter(**result)

    @abstractmethod
    def _get_search_space_config(self):
        """Arguments for `tune` function."""
        pass


def _fit_estimator(estimator, *args, **kwargs):
    estimator = sklearn.clone(estimator)

    start_time = time.time()
    estimator.fit(*args, **kwargs)
    fit_time = time.time() - start_time

    return fit_time, estimator


class GridSearchCV(BaseSearchCV):
    """Exhaustive search over specified parameter values for an estimator.

    Important members are fit, predict.

    GridSearchCV implements a "fit" and a "score" method.
    It also implements "predict", "predict_proba", "decision_function",
    "transform" and "inverse_transform" if they are implemented in the
    estimator used.

    The parameters of the estimator used to apply these methods are optimized
    by cross-validated grid-search over a parameter grid.

    Parameters
    ----------
    estimator : estimator object.
        This is assumed to implement the scikit-learn estimator interface.
        Either estimator needs to provide a ``score`` function,
        or ``scoring`` must be passed.

    param_grid : dict
        Dictionary with parameters names (string) as keys and lists of
        parameter settings to try as values.
        This enables searching over any sequence of parameter settings.

    scoring : string, callable, list/tuple, dict or None, default: None
        A single string (see :ref:`scoring_parameter`) or a callable
        (see :ref:`scoring`) to evaluate the predictions on the test set.

        For evaluating multiple metrics, either give a list of (unique) strings
        or a dict with names as keys and callables as values.

        NOTE that when using custom scorers, each scorer should return a single
        value. Metric functions returning a list/array of values can be wrapped
        into multiple scorers that return one value each.

        If None, the estimator's score method is used.

    cv : int, cross-validation generator or an iterable, optional
        Determines the cross-validation splitting strategy.
        Possible inputs for cv are:

        - None, to use the default 3-fold cross validation,
        - integer, to specify the number of folds in a `(Stratified)KFold`,
        - An iterable yielding (train, test) splits as arrays of indices.

        For integer/None inputs, if the estimator is a classifier and ``y`` is
        either binary or multiclass, :class:`StratifiedKFold` is used. In all
        other cases, :class:`KFold` is used.

    refit : boolean, string, or callable, default=True
        Refit an estimator using the best found parameters on the whole
        dataset.

        For multiple metric evaluation, this needs to be a string denoting the
        scorer that would be used to find the best parameters for refitting
        the estimator at the end.

        Where there are considerations other than maximum score in
        choosing a best estimator, ``refit`` can be set to a function which
        returns the selected ``best_index_`` given ``cv_results_``.

        The refitted estimator is made available at the ``best_estimator_``
        attribute and permits using ``predict`` directly on this
        ``GridSearchCV`` instance.

        Also for multiple metric evaluation, the attributes ``best_index_``,
        ``best_score_`` and ``best_params_`` will only be available if
        ``refit`` is set and all of them will be determined w.r.t this specific
        scorer. ``best_score_`` is not returned if refit is callable.

        See ``scoring`` parameter to know more about multiple metric
        evaluation.

        The refit is performed on Ray.

    verbose : integer
        Controls the verbosity: the higher, the more messages.

    error_score : 'raise' or numeric
        Value to assign to the score if an error occurs in estimator fitting.
        If set to 'raise', the error is raised. If a numeric value is given,
        FitFailedWarning is raised. This parameter does not affect the refit
        step, which will always raise the error. Default is 'raise' but from
        version 0.22 it will change to np.nan.

    return_train_score : boolean, default=False
        If ``False``, the ``cv_results_`` attribute will not include training
        scores.
        Computing training scores is used to get insights on how different
        parameter settings impact the overfitting/underfitting trade-off.
        However computing the scores on the training set can be computationally
        expensive and is not strictly required to select the parameters that
        yield the best generalization performance.

    resources_per_trial : dict of string -> integer
        Resource to use per trial. Accepted keys are `cpu`, `gpu` and custom
        resources.

    Examples
    --------
    >>>
    >> from sklearn import svm, datasets
    >> from tune_x import GridSearchCV
    >> iris = datasets.load_iris()
    >> parameters = {'kernel':['linear', 'rbf'], 'C':[1, 10]}
    >> svc = svm.SVC(gamma="scale")
    >> clf = GridSearchCV(estimator=svc, param_grid=parameters, cv=5)
    >> clf.fit(iris.data, iris.target)

    Attributes
    ----------
    cv_results_ : dict of numpy ndarrays
        A dict with keys as column headers and values as columns, that can be
        imported into a pandas ``DataFrame``.

        NOTE

        The key ``'params'`` is used to store a list of parameter
        settings dicts for all the parameter candidates.

        The ``mean_fit_time``, ``std_fit_time``, ``mean_score_time`` and
        ``std_score_time`` are all in seconds.

        For multi-metric evaluation, the scores for all the scorers are
        available in the ``cv_results_`` dict at the keys ending with that
        scorer's name (``'_<scorer_name>'``) instead of ``'_score'`` shown
        above. ('split0_test_precision', 'mean_train_precision' etc.)

    best_estimator_ : estimator or dict
        Estimator that was chosen by the search, i.e. estimator
        which gave highest score (or smallest loss if specified)
        on the left out data. Not available if ``refit=False``.

        See ``refit`` parameter for more information on allowed values.

    best_score_ : float
        Mean cross-validated score of the best_estimator

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    best_params_ : dict
        Parameter setting that gave the best results on the hold out data.

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    best_index_ : int
        The index (of the ``cv_results_`` arrays) which corresponds to the best
        candidate parameter setting.

        The dict at ``search.cv_results_['params'][search.best_index_]`` gives
        the parameter setting for the best model, that gives the highest
        mean score (``search.best_score_``).

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    scorer_ : function or a dict
        Scorer function used on the held out data to choose the best
        parameters for the model.

        For multi-metric evaluation, this attribute holds the validated
        ``scoring`` dict which maps the scorer key to the scorer callable.

    n_splits_ : int
        The number of cross-validation splits (folds/iterations).

    refit_time_ : float
        Seconds used for refitting the best model on the whole dataset.

        This is present only if ``refit`` is not False.

    Notes
    -----
    The parameters selected are those that maximize the score of the left out
    data, unless an explicit score is passed in which case it is used instead.
    """

    def __init__(
            self,
            *,
            estimator,
            param_grid,
            scoring=None,
            refit=True,
            cv=5,
            verbose=0,
            error_score=np.nan,
            return_train_score=False,
            resources_per_trial=None,
    ):
        super().__init__(
            estimator=estimator,
            scoring=scoring,
            refit=refit,
            cv=cv,
            verbose=verbose,
            error_score=error_score,
            return_train_score=return_train_score,
            resources_per_trial=resources_per_trial,
        )
        self.param_grid = param_grid

        if isinstance(param_grid, list):
            raise NotImplementedError("Multiple grids not supported")

    def _get_search_space_config(self):
        return {
            "config": {
                k: tune.grid_search(v)
                for k, v in self.param_grid.items()
            }
        }


class RandomizedSearchCV(BaseSearchCV):
    """Randomized search on hyper parameters.

    RandomizedSearchCV implements a "fit" and a "score" method.
    It also implements "predict", "predict_proba", "decision_function",
    "transform" and "inverse_transform" if they are implemented in the
    estimator used.

    The parameters of the estimator used to apply these methods are optimized
    by cross-validated grid-search over a parameter grid.

    In contrast to GridSearchCV, not all parameter values are tried out, but
    rather a fixed number of parameter settings is sampled from the specified
    distributions. The number of parameter settings that are tried is
    given by n_iter.

    Parameters
    ----------
    estimator : estimator object.
        This is assumed to implement the scikit-learn estimator interface.
        Either estimator needs to provide a ``score`` function,
        or ``scoring`` must be passed.

    param_distributions : dict
        Dictionary with parameters names (string) as keys and lists of
        parameter settings to try as values.
        This enables searching over any sequence of parameter settings.

    n_iter : int, default=10
        Number of parameter settings that are sampled. n_iter trades
        off runtime vs quality of the solution.

    scoring : string, callable, list/tuple, dict or None, default: None
        A single string (see :ref:`scoring_parameter`) or a callable
        (see :ref:`scoring`) to evaluate the predictions on the test set.

        For evaluating multiple metrics, either give a list of (unique) strings
        or a dict with names as keys and callables as values.

        NOTE that when using custom scorers, each scorer should return a single
        value. Metric functions returning a list/array of values can be wrapped
        into multiple scorers that return one value each.

        If None, the estimator's score method is used.

    cv : int, cross-validation generator or an iterable, optional
        Determines the cross-validation splitting strategy.
        Possible inputs for cv are:

        - None, to use the default 3-fold cross validation,
        - integer, to specify the number of folds in a `(Stratified)KFold`,
        - An iterable yielding (train, test) splits as arrays of indices.

        For integer/None inputs, if the estimator is a classifier and ``y`` is
        either binary or multiclass, :class:`StratifiedKFold` is used. In all
        other cases, :class:`KFold` is used.

    refit : boolean, string, or callable, default=True
        Refit an estimator using the best found parameters on the whole
        dataset.

        For multiple metric evaluation, this needs to be a string denoting the
        scorer that would be used to find the best parameters for refitting
        the estimator at the end.

        Where there are considerations other than maximum score in
        choosing a best estimator, ``refit`` can be set to a function which
        returns the selected ``best_index_`` given ``cv_results_``.

        The refitted estimator is made available at the ``best_estimator_``
        attribute and permits using ``predict`` directly on this
        ``GridSearchCV`` instance.

        Also for multiple metric evaluation, the attributes ``best_index_``,
        ``best_score_`` and ``best_params_`` will only be available if
        ``refit`` is set and all of them will be determined w.r.t this specific
        scorer. ``best_score_`` is not returned if refit is callable.

        See ``scoring`` parameter to know more about multiple metric
        evaluation.

        The refit is performed on Ray.

    verbose : integer
        Controls the verbosity: the higher, the more messages.

    error_score : 'raise' or numeric
        Value to assign to the score if an error occurs in estimator fitting.
        If set to 'raise', the error is raised. If a numeric value is given,
        FitFailedWarning is raised. This parameter does not affect the refit
        step, which will always raise the error. Default is 'raise' but from
        version 0.22 it will change to np.nan.

    return_train_score : boolean, default=False
        If ``False``, the ``cv_results_`` attribute will not include training
        scores.
        Computing training scores is used to get insights on how different
        parameter settings impact the overfitting/underfitting trade-off.
        However computing the scores on the training set can be computationally
        expensive and is not strictly required to select the parameters that
        yield the best generalization performance.

    resources_per_trial : dict of string -> integer
        Resource to use per trial. Accepted keys are `cpu`, `gpu` and custom
        resources.

    Attributes
    ----------
    cv_results_ : dict of numpy ndarrays
        A dict with keys as column headers and values as columns, that can be
        imported into a pandas ``DataFrame``.

        NOTE

        The key ``'params'`` is used to store a list of parameter
        settings dicts for all the parameter candidates.

        The ``mean_fit_time``, ``std_fit_time``, ``mean_score_time`` and
        ``std_score_time`` are all in seconds.

        For multi-metric evaluation, the scores for all the scorers are
        available in the ``cv_results_`` dict at the keys ending with that
        scorer's name (``'_<scorer_name>'``) instead of ``'_score'`` shown
        above. ('split0_test_precision', 'mean_train_precision' etc.)

    best_estimator_ : estimator or dict
        Estimator that was chosen by the search, i.e. estimator
        which gave highest score (or smallest loss if specified)
        on the left out data. Not available if ``refit=False``.

        See ``refit`` parameter for more information on allowed values.

    best_score_ : float
        Mean cross-validated score of the best_estimator

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    best_params_ : dict
        Parameter setting that gave the best results on the hold out data.

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    best_index_ : int
        The index (of the ``cv_results_`` arrays) which corresponds to the best
        candidate parameter setting.

        The dict at ``search.cv_results_['params'][search.best_index_]`` gives
        the parameter setting for the best model, that gives the highest
        mean score (``search.best_score_``).

        For multi-metric evaluation, this is present only if ``refit`` is
        specified.

    scorer_ : function or a dict
        Scorer function used on the held out data to choose the best
        parameters for the model.

        For multi-metric evaluation, this attribute holds the validated
        ``scoring`` dict which maps the scorer key to the scorer callable.

    n_splits_ : int
        The number of cross-validation splits (folds/iterations).

    refit_time_ : float
        Seconds used for refitting the best model on the whole dataset.

        This is present only if ``refit`` is not False.

    Notes
    -----
    The parameters selected are those that maximize the score of the left out
    data, unless an explicit score is passed in which case it is used instead.
    """

    def __init__(
            self,
            *,
            estimator,
            param_distributions,
            n_iter=10,
            scoring=None,
            refit=True,
            cv=5,
            verbose=0,
            error_score=np.nan,
            return_train_score=False,
            resources_per_trial=None,
    ):
        super().__init__(
            estimator=estimator,
            scoring=scoring,
            refit=refit,
            cv=cv,
            verbose=verbose,
            error_score=error_score,
            return_train_score=return_train_score,
            resources_per_trial=resources_per_trial,
        )
        self.param_distributions = param_distributions
        self.n_iter = n_iter

        if isinstance(param_distributions, list):
            raise NotImplementedError("Multiple grids not supported")

    def _get_search_space_config(self):
        return {
            "config": {
                k: tune.choice(np.array(v, dtype=object))
                for k, v in self.param_distributions.items()
            },
            "num_samples": self.n_iter,
        }
