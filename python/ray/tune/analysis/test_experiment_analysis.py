from experiment_analysis import *
import pandas as pd

nevergrad_analysis = ExperimentAnalysis("~adizim/ray_results/nevergrad")
#print(nevergrad_analysis.checkpoints()[0])
print(nevergrad_analysis.dataframe())
print(nevergrad_analysis.trial_dataframe("f5234953"))
#print(nevergrad_analysis.get_best_trainable("neg_mean_loss"))
#print(nevergrad_analysis.trials())
#print(nevergrad_analysis.stats())