import os
from pathlib import Path

from syndiffix_tools.common_tasks import *
from syndiffix_tools.tables_manager import TablesManager
import shutil
import numpy as np

import pprint
pp = pprint.PrettyPrinter(indent=4)

"""
Demonstrates usage of TablesManager to manage storage and naming of
synthetic data files
"""
def get_generic_dataframe():
    np.random.seed(0)
    n = 100
    df = pd.DataFrame(
        {
            "str5": np.random.choice(list("abcde"), n),
            "int10": np.random.randint(1, 11, n),
            "float": np.random.randn(n),
            "datetime": pd.date_range("2000-01-01", "2020-01-01", periods=n),
        }
    )
    df["pid"] = df.index
    return df

# Make a dataframe
df = get_generic_dataframe()
test_path = Path("tests/test_dir")

# Make the directory where the tables and other metadat will be stored
os.makedirs(test_path, exist_ok=True)

# remove all files from the directory test_path so that we have a clean
# start to this code
if os.path.exists(test_path):
    shutil.rmtree(test_path)

# Create directory test_path if it does not exist
os.makedirs(test_path, exist_ok=True)

# Create the TablesManager object
tm = TablesManager(dir_path=test_path)

# Assign the original data to TablesManager and put it in the directory
tm.put_df_orig(df, "test_file", also_make_csv=True)

# The original dataframe is available at tm.df_orig
print(list(tm.df_orig.columns))

# At this point, the metadata file at
# "tests/test_dir/orig_meta_data.json" can be edited as needed
# However, if there is a pid column, then it must be set before synthesizing
# (either with set_pid_cols or through direct editing of the metadata file)
tm.set_pid_cols(["pid"])

# Make a synthetic table with the following two columns.
# Various statistics about the synthesis internals are also saved in a separate file.
tm.synthesize(columns=["datetime", "str5"], also_save_stats=True)

# Create synthetic table with all columns
tm.synthesize(also_save_stats=True)

# Create one more synthetic table for fun
tm.synthesize(columns=["datetime", "float"], also_save_stats=True)

# We can check to see if a synthetic table with certain columns exists
if not tm.syn_file_exists(columns=["str5", "datetime"]):
    print(f"Synthetic table with columns datetime and str5 should exist but doesn't")
    quit()

if tm.syn_file_exists(columns=["float", "str5"]):
    print(f"Synthetic table with columns float and str5 appears to exist but should not")
    quit()

# We can also get the synthetic table as a dataframe
syn_df = tm.get_syn_df(columns=["datetime", "str5"])
if syn_df is None:
    print(f"Synthetic table with columns datetime and str5 should have been returned")
    quit()
print(syn_df.head())

syn_df = tm.get_syn_df(columns=["float", "str5"])
if syn_df is not None:
    print(f"Synthetic table with columns float and str5 should not have been returned")
    quit()

# We can get the dataframe that best matches the columns we want
# The returned dataframe will have at least the columns we ask for, but
# may have additional columns
syn_df = tm.get_best_syn_df(columns=["float", "str5"])
if syn_df is None:
    print(f"We should have received the full synthetic table when requesting columns float and str5")
    quit()
print(syn_df.head())

# Note that internally, TablesManager builds a catalog to keep track of all
# of the synthetic tables that have been created.
pp.pprint(tm.catalog)