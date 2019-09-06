"""
  A demo on an electrolyte optimization task using Optimizer.
  Example based off of 
  https://github.com/dragonfly/dragonfly/blob/master/examples/detailed_use_cases/in_code_demo_single_objective.py
"""

from __future__ import print_function
from argparse import Namespace
from ray.tune.integration import TuneOptimizer
import numpy as np
import matplotlib.pyplot as plt

# Objective 3D function from Dragonfly
def compute_objectives(x):
    """ Computes the objectives. """
    vol1 = x[0] # LiNO3
    vol2 = x[1] # Li2SO4
    vol3 = x[2] # NaClO4
    vol4 = 10 - (vol1 + vol2 + vol3) # Water
    # Synthetic functions
    conductivity = vol1 + 0.1 * (vol2 + vol3) ** 2 + 2.3 * vol4 * (vol1 ** 1.5)
    voltage_window = 0.5 * (vol1 + vol2) ** 1.7 + 1.2 * (vol3 ** 0.5) * (vol1 ** 1.5)
    # Add Gaussian noise to simulate experimental noise
    conductivity += np.random.normal() * 0.01
    voltage_window += np.random.normal() * 0.01
    return [conductivity, voltage_window]

def objective(x):
    """ Computes the objectives. """
    return compute_objectives(x)[0] # Just returns conductivity


def main():
    """ Main function. """
    optimizer = TuneOptimizer([(0, 7.25), (0, 7.25), (0, 7.25)])
    points = optimizer.ask(n_points=10)
    y_vals = []
    for point in points:
        y_vals.append(objective(point))
    print(points, y_vals)
    optimizer.tell(points, y_vals)


if __name__ == '__main__':
    main()