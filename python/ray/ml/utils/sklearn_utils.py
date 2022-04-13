import numbers
import warnings
from contextlib import suppress
from traceback import format_exc

from sklearn.base import BaseEstimator
from sklearn.metrics._scorer import _MultimetricScorer

# thread_count is a catboost parameter
SKLEARN_CPU_PARAM_NAMES = ["n_jobs", "thread_count"]


def has_cpu_params(estimator: BaseEstimator) -> bool:
    """Returns True if estimator has any CPU-related params."""
    return any(
        any(
            param.endswith(cpu_param_name) for cpu_param_name in SKLEARN_CPU_PARAM_NAMES
        )
        for param in estimator.get_params(deep=True)
    )


def set_cpu_params(estimator: BaseEstimator, num_cpus: int) -> None:
    """Sets all CPU-related params to num_cpus (incl. nested)."""
    cpu_params = {
        param: num_cpus
        for param in estimator.get_params(deep=True)
        if any(
            param.endswith(cpu_param_name) for cpu_param_name in SKLEARN_CPU_PARAM_NAMES
        )
    }
    estimator.set_params(**cpu_params)


# score func from https://github.com/scikit-learn/scikit-learn/blob/\
# d7c84b239c18a4627e6af7c369a504cb7609bfcd/sklearn/model_selection/_validation.py
# Under BSD 3-Clause License

# Copyright (c) 2007-2021 The scikit-learn developers.
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


def score(estimator, X_test, y_test, scorer, error_score="raise"):
    """Compute the score(s) of an estimator on a given test set.
    Will return a dict of floats if `scorer` is a dict, otherwise a single
    float is returned.
    """
    if isinstance(scorer, dict):
        # will cache method calls if needed. scorer() returns a dict
        scorer = _MultimetricScorer(**scorer)

    try:
        if y_test is None:
            scores = scorer(estimator, X_test)
        else:
            scores = scorer(estimator, X_test, y_test)
    except Exception:
        if error_score == "raise":
            raise
        else:
            if isinstance(scorer, _MultimetricScorer):
                scores = {name: error_score for name in scorer._scorers}
            else:
                scores = error_score
            warnings.warn(
                "Scoring failed. The score on this train-test partition for "
                f"these parameters will be set to {error_score}. Details: \n"
                f"{format_exc()}",
                UserWarning,
            )

    error_msg = "scoring must return a number, got %s (%s) instead. (scorer=%s)"
    if isinstance(scores, dict):
        for name, score in scores.items():
            if hasattr(score, "item"):
                with suppress(ValueError):
                    # e.g. unwrap memmapped scalars
                    score = score.item()
            if not isinstance(score, numbers.Number):
                raise ValueError(error_msg % (score, type(score), name))
            scores[name] = score
    else:  # scalar
        if hasattr(scores, "item"):
            with suppress(ValueError):
                # e.g. unwrap memmapped scalars
                scores = scores.item()
        if not isinstance(scores, numbers.Number):
            raise ValueError(error_msg % (scores, type(scores), scorer))
    return scores
