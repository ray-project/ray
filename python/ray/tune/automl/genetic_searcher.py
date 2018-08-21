#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: yiguang.wyg (yiguang.wyg@antfin.com)
# date: 2018/06/27

import logging
import numpy as np

from .search_policy import AutoMLSearcher


class GeneticSearch(AutoMLSearcher):
    """Implement the genetic search.

    Keep a collection of top-K parameter permutations as base genes,
    then apply selection, crossover, and mutation to them to generate
    new genes (a.k.a new generation). Hopefully, the performance of
    the top population would increase generation by generation.
    """

    def __init__(self, search_space, reward_attr,
                 max_generation=2,
                 population_size=10,
                 keep_top_ratio=0.2,
                 selection_bound=0.4,
                 crossover_bound=0.6):
        """
        Initialize GeneticSearcher.

        Args:
            search_space (SearchSpace): The space to search.
            reward_attr: The attribute name of the reward in the result.
            max_generation: Max iteration number of genetic search.
            population_size: Number of trials of the initial generation.
            keep_top_ratio: Ratio of the top performance population.
            selection_bound: Threshold for performing selection.
            crossover_bound: Threshold for performing crossover.
        """
        super(GeneticSearch, self).__init__(search_space, reward_attr)

        self._cur_generation = 1
        self._max_generation = max_generation
        self._population_size = population_size
        self._keep_top_ratio = keep_top_ratio
        self._selection_bound = selection_bound
        self._crossover_bound = crossover_bound

        self._cur_config_list = []
        self._cur_encoding_list = []
        for _ in range(population_size):
            one_hot = self.search_space.generate_random_one_hot_encoding()
            self._cur_encoding_list.append(one_hot)
            self._cur_config_list.append(
                self.search_space.apply_one_hot_encoding(one_hot))

    def _select(self):
        population_size = len(self._cur_config_list)
        logging.info("The %sth generation, population=%s"
                     % (self._cur_generation, population_size))
        return self._cur_config_list, self._cur_encoding_list

    def _feedback(self, trials):
        self._cur_generation += 1
        if self._cur_generation > self._max_generation:
            return AutoMLSearcher.TERMINATE

        sorted_trials = sorted(
            trials, key=lambda t: t.best_result[self.reward_attr], reverse=True)
        self._cur_encoding_list = self._next_generation(sorted_trials)
        self._cur_config_list = []
        for one_hot in self._cur_encoding_list:
            self._cur_config_list.append(
                self.search_space.apply_one_hot_encoding(one_hot))

        return AutoMLSearcher.CONTINUE

    def _next_generation(self, sorted_trials):
        """Generate genes (encodings) for the next generation.

        Args：
            sorted_trials: List of finished trials with top performance ones first.

        Returns:
            A list of new genes (encodings)
        """

        candidate = []
        next_generation = []
        num_population = self._next_population_size(len(sorted_trials))
        top_num = int(max(num_population * self._keep_top_ratio, 2))

        for i in range(top_num):
            candidate.append(sorted_trials[i].extra_arg)
            next_generation.append(sorted_trials[i].extra_arg)

        for i in range(top_num, num_population):
            flip_coin = np.random.uniform()
            if flip_coin >= self._selection_bound:
                next_generation.append(GeneticSearch._selection(candidate))
            else:
                flip_coin = np.random.uniform()
                if flip_coin >= self._crossover_bound:
                    next_generation.append(GeneticSearch._crossover(candidate))
                else:
                    next_generation.append(GeneticSearch._mutation(candidate))
        return next_generation

    def _next_population_size(self, last_population_size):
        """Calculate the population size of the next generation.

        Intuitively, the population should decay after each iteration since
        it should converge. It can also decrease the total resource required.

        Args:
            last_population_size: The last population size.

        Returns:
            The new population size.
        """
        # TODO: implement an generic resource allocate algorithm.
        return int(max(last_population_size * 0.9, 3))

    @staticmethod
    def _selection(candidate):
        """Perform selection action to candidates.

        For example, new gene = sample_1 + the 5th bit of sample2.

        Args:
            candidate: List of candidate genes (encodings).

        Returns:
            New gene (encoding)
        """
        sample_index1 = np.random.choice(len(candidate))
        sample_index2 = np.random.choice(len(candidate))
        sample_1 = candidate[sample_index1]
        sample_2 = candidate[sample_index2]
        select_index = np.random.choice(len(sample_1))
        logging.info("Perform selection from %sth to %sth at index=%s"
                     % (sample_index2, sample_index1, select_index))

        next_gen = []
        for i in range(len(sample_1)):
            if i is select_index:
                next_gen.append(sample_2[i])
            else:
                next_gen.append(sample_1[i])
        return next_gen

    @staticmethod
    def _crossover(candidate):
        """Perform crossover action to candidates.

        For example, new gene = 60% sample_1 + 40% sample_2.

        Args:
            candidate: List of candidate genes (encodings).

        Returns:
            New gene (encoding)
        """
        sample_index1 = np.random.choice(len(candidate))
        sample_index2 = np.random.choice(len(candidate))
        sample_1 = candidate[sample_index1]
        sample_2 = candidate[sample_index2]
        cross_index = int(len(sample_1) * np.random.uniform(low=0.3, high=0.7))
        logging.info("Perform crossover between %sth and %sth at index=%s"
                     % (sample_index1, sample_index2, cross_index))

        next_gen = []
        for i in range(len(sample_1)):
            if i > cross_index:
                next_gen.append(sample_2[i])
            else:
                next_gen.append(sample_1[i])
        return next_gen

    @staticmethod
    def _mutation(candidate, rate=0.1):
        """Perform mutation action to candidates.

        For example, randomly change 10% of original sample

        Args:
            candidate: List of candidate genes (encodings).
            rate: Percentage of mutation bits

        Returns:
            New gene (encoding)
        """
        sample_index = np.random.choice(len(candidate))
        sample = candidate[sample_index]
        idx_list = []
        for i in range(int(max(len(sample) * rate, 1))):
            idx = np.random.choice(len(sample))
            idx_list.append(idx)

            field = sample[idx]       # one-hot encoding
            field[np.argmax(field)] = 0
            bit = np.random.choice(field.shape[0])
            field[bit] = 1

        logging.info("Perform mutation on %sth at index=%s"
                     % (sample_index, str(idx_list)))
        return sample
