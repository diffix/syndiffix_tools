import json
import os
from pathlib import Path

from syndiffix_tools.tables_builder import TablesBuilder
from syndiffix_tools.common_tasks import *

from helpers import *

def test_input_new_df_orig():
    df = get_generic_dataframe()
    test_path = Path("tests/test_dir")
    os.makedirs(test_path, exist_ok=True)
    # remove all files from the directory test_path
    for file in os.listdir(test_path):
        os.remove(os.path.join(test_path, file))
    tb = TablesBuilder(dir_path=test_path)
    tb.put_df_orig(df, "test_file", also_make_csv=True)
    assert tb.df_orig.equals(df)
    assert Path(test_path, "orig_meta_data.json").exists()
    with open(Path(test_path, "orig_meta_data.json"), "r") as file:
        orig_meta_data = json.load(file)
        assert tb.orig_meta_data == orig_meta_data


def test_input_existing_df_orig():
    # must run after test_input_new_df_orig
    test_path = Path("tests/test_dir")
    assert Path(test_path, "orig_meta_data.json").exists()
    assert Path(test_path, "test_file.parquet").exists()
    assert Path(test_path, "test_file.csv").exists()
    tb = TablesBuilder(dir_path=test_path)
    with open(Path(test_path, "orig_meta_data.json"), "r") as file:
        orig_meta_data = json.load(file)
        assert tb.orig_meta_data == orig_meta_data
    df = get_df_from_pq(Path(test_path, "test_file.parquet"))
    assert tb.df_orig.equals(df)
    assert tb.orig_meta_data["orig_file_name"] == "test_file.parquet"
    assert tb.orig_meta_data["column_classes"]["str5"] == "categorical"


def test_set_pid_cols():
    # must run after test_input_new_df_orig
    test_path = Path("tests/test_dir")
    tb = TablesBuilder(dir_path=test_path)
    tb.set_pid_cols(["pid", "str5"])
    assert tb.orig_meta_data["pid_cols"] == ["pid", "str5"]
    tb.set_pid_cols(["str5"])
    assert tb.orig_meta_data["pid_cols"] == ["str5"]
    tb.set_pid_cols([])
    assert tb.orig_meta_data["pid_cols"] == []
    tb.set_pid_cols(["pid"])
    assert tb.orig_meta_data["pid_cols"] == ["pid"]