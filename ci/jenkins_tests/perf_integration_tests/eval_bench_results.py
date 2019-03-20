import json
import pandas
import sys

json_obj_old = json.loads(open("previous_benchmarks.json", "r").read())
incomplete_dataframe_old = pandas.DataFrame(json_obj_old["benchmarks"]).set_index("name")
complete_dataframe_old = pandas.concat([pandas.DataFrame(incomplete_dataframe_old.loc[i, "stats"], index=[i]) for i in incomplete_dataframe_old.index])

json_obj_new = json.loads(open("current_benchmarks.json", "r").read())
incomplete_dataframe_new = pandas.DataFrame(json_obj_new["benchmarks"]).set_index("name")
complete_dataframe_new = pandas.concat([pandas.DataFrame(incomplete_dataframe_new.loc[i, "stats"], index=[i]) for i in incomplete_dataframe_new.index])

if not all((complete_dataframe_old["mean"] - complete_dataframe_new["mean"]) > -1 * (0.05 * complete_dataframe_old["mean"])):
    sys.exit(-1)
else:
    sys.exit(0)
