from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import logging
import numpy as np

from ray.tune import grid_search

logger = logging.getLogger(__name__)


class ParameterSpace(object):
    """Base class of a single parameter's search space.
    """

    def __init__(self, name):
        """Initialize ParameterSpace.

        Arguments:
            name (str): Name of the parameter. Name can be dot separated,
                which will be interpreted as path of a nested config
        """
        self.name = name


class DiscreteSpace(ParameterSpace):
    """Search space with discrete choices.
    """

    def __init__(self, name, choices):
        """Initialize DiscreteSpace.

        Arguments:
            name (str): Name of the parameter.
            choices (list): List of all possible choices.
        """
        super(DiscreteSpace, self).__init__(name)
        self.choices = choices

    def to_grid_search(self):
        """Returns a ray.tune.grid_search structure.

        Contains all the choices inside and can be expanded by ray.tune.
        """
        return grid_search(self.choices)

    def to_random_choice(self):
        """Returns a lambda function that choose a value randomly.

        Can be expanded by ray.tune.
        """
        return lambda _: random.choice(self.choices)

    def choices_count(self):
        return len(self.choices)

    def __str__(self):
        return "DiscreteSpace %s: %s" % (self.name, str(self.choices))


class ContinuousSpace(ParameterSpace):
    """Search space of continuous type.

    NOTE that it can be converted to ``DiscreteSpace`` by sampling under
    certain distribution such as linear.
    """

    LINEAR = 'linear'

    # TODO: logspace

    def __init__(self, name, start, end, num, distribution=LINEAR):
        """Initialize ContinuousSpace.

        Arguments:
            name (str): Name of the parameter.
            start: Start of the continuous space included.
            end: End of the continuous space included.
            num: Sampling count if possible.
            distribution: Sampling distribution, should be in [LINEAR]
        """
        super(ContinuousSpace, self).__init__(name)
        self.start = float(start)
        self.end = float(end)
        self.num = num

        if distribution == ContinuousSpace.LINEAR:
            self.choices = np.linspace(start, end, num)
        else:
            raise NotImplementedError(
                "Distribution %s not supported" % distribution)

        self.distribution = distribution

    def to_grid_search(self):
        """Returns a ray.tune.grid_search structure.

        Apply sampling to get discrete choices.
        """
        return grid_search(self.choices)

    def to_random_choice(self):
        """Returns a lambda function that choose a value randomly.

        Can be expanded by ray.tune.
        """
        return lambda _: random.uniform(self.start, self.end)

    def choices_count(self):
        return len(self.choices)

    def __str__(self):
        return "ContinuousSpace %s: [%s, %s]" % (self.name, self.start,
                                                 self.end)


class SearchSpace(object):
    """Collection of ``ParameterSpace``, a.k.a <name, space> pair.

    It's supposed to be used with a fixed experiment config, which
    could be a very complicated (nested) dict. Each ``ParameterSpace``
    points to a unique place in the experiment config using its name
    as the path.
    """

    def __init__(self, param_list):
        """Initialize SearchSpace.

        Arguments:
            param_list: List of ``ParameterSpace`` (or its subclass).
        """
        self.param_list = param_list
        for ps in param_list:
            # ps MUST be ParameterSpace
            logger.info("Add %s into SearchSpace" % ps)

    def to_grid_search(self):
        """Returns a dict of {parameter name: grid_search}.

        Apply ``to_grid_search`` to all ``ParameterSpace``.
        """
        return {ps.name: ps.to_grid_search() for ps in self.param_list}

    def to_random_choice(self):
        """Returns a dict of {parameter name: lambda function}.

        Apply ``to_grid_search`` to all ``ParameterSpace``.
        """
        return {ps.name: ps.to_random_choice() for ps in self.param_list}

    def generate_random_one_hot_encoding(self):
        """Returns a list of one-hot encodings for all parameters.

        1 one-hot np.array for 1 parameter,
        and the 1's place is randomly chosen.
        """
        encoding = []
        for ps in self.param_list:
            one_hot = np.zeros(ps.choices_count())
            choice = random.randrange(ps.choices_count())
            one_hot[choice] = 1
            encoding.append(one_hot)
        return encoding

    def apply_one_hot_encoding(self, one_hot_encoding):
        """Apply one hot encoding to generate a specific config.


        Arguments:
            one_hot_encoding (list): A list of one hot encodings,
                1 for each parameter. The shape of each encoding
                should match that ``ParameterSpace``

        Returns:
            A dict config with specific <name, value> pair
        """
        config = {}
        for ps, one_hot in zip(self.param_list, one_hot_encoding):
            index = np.argmax(one_hot)
            config[ps.name] = ps.choices[index]
        return config
