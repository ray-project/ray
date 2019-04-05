from experiment_analysis import *
import pandas as pd

nevergrad_analysis = ExperimentAnalysis("~adizim/ray_results/nevergrad")
print(nevergrad_analysis.dataframe())
print(nevergrad_analysis.trials())
print(nevergrad_analysis.stats())