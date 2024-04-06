import os
from pathlib import Path

from common_tasks import *
from tables_manager import TablesManager
from tests.helpers import *

"""
Demonstrates usage of TablesManager to manage storage and naming of
synthetic data files
"""

# Make a dataframe
df = get_generic_dataframe()
test_path = Path("tests/test_dir")

# Make the directory where the tables and other metadat will be stored
os.makedirs(test_path, exist_ok=True)

# remove all files from the directory test_path in case something already there
for file in os.listdir(test_path):
    os.remove(os.path.join(test_path, file))

# Create the TablesManager object
tm = TablesManager(dir_path=test_path)

# Assign the original data to TablesManager and put it in the directory
tm.put_df_orig(df, "test_file", also_make_csv=True)

# At this point, the metadata file at
# "tests/test_dir/orig_meta_data.json" can be edited as needed
# However, if there is a pid column, then it must be set before synthesizing
# (either with set_pid_cols or through direct editing of the metadata file)
tm.set_pid_cols(["pid"])

# Make a synthetic table with the following two columns.
# Various statistics about the synthesis internals are also saved in a separate file.
tm.synthesize(columns=["datetime", "str5"], also_save_stats=True)

# Additional synthetic tables may be created after this
