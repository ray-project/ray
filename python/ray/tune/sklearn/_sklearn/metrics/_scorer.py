from sklearn.metrics import check_scoring


def _check_multimetric_scoring(estimator, scoring=None):
    """Check the scoring parameter in cases when multiple metrics are allowed

    Parameters
    ----------
    estimator : sklearn estimator instance
        The estimator for which the scoring will be applied.

    scoring : string, callable, list/tuple, dict or None, default: None
        A single string (see :ref:`scoring_parameter`) or a callable
        (see :ref:`scoring`) to evaluate the predictions on the test set.

        For evaluating multiple metrics, either give a list of (unique) strings
        or a dict with names as keys and callables as values.

        NOTE that when using custom scorers, each scorer should return a single
        value. Metric functions returning a list/array of values can be wrapped
        into multiple scorers that return one value each.

        See :ref:`multimetric_grid_search` for an example.

        If None the estimator's score method is used.
        The return value in that case will be ``{'score': <default_scorer>}``.
        If the estimator's score method is not available, a ``TypeError``
        is raised.

    Returns
    -------
    scorers_dict : dict
        A dict mapping each scorer name to its validated scorer.

    is_multimetric : bool
        True if scorer is a list/tuple or dict of callables
        False if scorer is None/str/callable
    """
    if callable(scoring) or scoring is None or isinstance(scoring, str):
        scorers = {"score": check_scoring(estimator, scoring=scoring)}
        return scorers, False
    else:
        err_msg_generic = (
            "scoring should either be a single string or "
            "callable for single metric evaluation or a "
            "list/tuple of strings or a dict of scorer name "
            "mapped to the callable for multiple metric "
            "evaluation. Got %s of type %s" % (repr(scoring), type(scoring)))

        if isinstance(scoring, (list, tuple, set)):
            err_msg = ("The list/tuple elements must be unique "
                       "strings of predefined scorers. ")
            invalid = False
            try:
                keys = set(scoring)
            except TypeError:
                invalid = True
            if invalid:
                raise ValueError(err_msg)

            if len(keys) != len(scoring):
                raise ValueError(err_msg + "Duplicate elements were found in"
                                 " the given list. %r" % repr(scoring))
            elif len(keys) > 0:
                if not all(isinstance(k, str) for k in keys):
                    if any(callable(k) for k in keys):
                        raise ValueError(err_msg +
                                         "One or more of the elements were "
                                         "callables. Use a dict of score name "
                                         "mapped to the scorer callable. "
                                         "Got %r" % repr(scoring))
                    else:
                        raise ValueError(
                            err_msg + "Non-string types were found in "
                            "the given list. Got %r" % repr(scoring))
                scorers = {
                    scorer: check_scoring(estimator, scoring=scorer)
                    for scorer in scoring
                }
            else:
                raise ValueError(err_msg +
                                 "Empty list was given. %r" % repr(scoring))

        elif isinstance(scoring, dict):
            keys = set(scoring)
            if not all(isinstance(k, str) for k in keys):
                raise ValueError("Non-string types were found in the keys of "
                                 "the given dict. scoring=%r" % repr(scoring))
            if len(keys) == 0:
                raise ValueError(
                    "An empty dict was passed. %r" % repr(scoring))
            scorers = {
                key: check_scoring(estimator, scoring=scorer)
                for key, scorer in scoring.items()
            }
        else:
            raise ValueError(err_msg_generic)
        return scorers, True
