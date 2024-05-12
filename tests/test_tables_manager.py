import json
import os
from pathlib import Path

from tables_manager import TablesManager
from common_tasks import *

from helpers import *

def test_input_new_df_orig():
    df = get_generic_dataframe()
    test_path = Path("tests/test_dir")
    os.makedirs(test_path, exist_ok=True)
    # remove all files from the directory test_path
    for file in os.listdir(test_path):
        os.remove(os.path.join(test_path, file))
    tm = TablesManager(dir_path=test_path)
    tm.put_df_orig(df, "test_file", also_make_csv=True)
    assert tm.df_orig.equals(df)
    assert Path(test_path, "orig_meta_data.json").exists()
    with open(Path(test_path, "orig_meta_data.json"), "r") as file:
        orig_meta_data = json.load(file)
        assert tm.orig_meta_data == orig_meta_data


def test_input_existing_df_orig():
    # must run after test_input_new_df_orig
    test_path = Path("tests/test_dir")
    assert Path(test_path, "orig_meta_data.json").exists()
    assert Path(test_path, "test_file.parquet").exists()
    assert Path(test_path, "test_file.csv").exists()
    tm = TablesManager(dir_path=test_path)
    with open(Path(test_path, "orig_meta_data.json"), "r") as file:
        orig_meta_data = json.load(file)
        assert tm.orig_meta_data == orig_meta_data
    df = get_df_from_pq(Path(test_path, "test_file.parquet"))
    assert tm.df_orig.equals(df)
    assert tm.orig_meta_data["orig_file_name"] == "test_file.parquet"
    assert tm.orig_meta_data["column_classes"]["str5"] == "categorical"


def test_set_pid_cols():
    # must run after test_input_new_df_orig
    test_path = Path("tests/test_dir")
    tm = TablesManager(dir_path=test_path)
    tm.set_pid_cols(["pid", "str5"])
    assert tm.orig_meta_data["pid_cols"] == ["pid", "str5"]
    tm.set_pid_cols(["str5"])
    assert tm.orig_meta_data["pid_cols"] == ["str5"]
    tm.set_pid_cols([])
    assert tm.orig_meta_data["pid_cols"] == []
    tm.set_pid_cols(["pid"])
    assert tm.orig_meta_data["pid_cols"] == ["pid"]