import json
import os
import time
from pathlib import Path
from typing import Union

import pandas as pd
from syndiffix.synthesizer import Synthesizer

from syndiffix_tools.cluster_info import *
from syndiffix_tools.common_tasks import *
from syndiffix_tools.tree_walker import *


class TablesManager:
    """
    This class takes an original dataset and helps manage the synthetic datasets that can be generated from it.

    Inputs:
        - dir_path: str or Path. the directory path where the synthetic
              datasets and other metadata are stored.
    Files:
        - orig_meta_data.json: metadata about the original dataset. This is initially created with a best guess as to whether columns are continuous or categorical. This can be manually edited afterwards.
    """

    def __init__(self, dir_path: Union[str, Path]) -> None:
        self.df_orig = None  # populate with put_df_orig
        self.orig_file_name = None
        self.orig_meta_data = {}
        if type(dir_path) == str:
            self.dir_path = Path(dir_path)
        else:
            self.dir_path = dir_path
        if not self.dir_path.exists():
            raise FileNotFoundError(f"Directory {self.dir_path} does not exist.")
        self.stats_dir_path = Path(self.dir_path, "stats")
        self.stats_dir_path.mkdir(exist_ok=True)
        self.syn_dir_path = Path(self.dir_path, "syn")
        self.syn_dir_path.mkdir(exist_ok=True)
        self.meta_data_path = Path(self.dir_path, "orig_meta_data.json")
        if self.meta_data_path.exists():
            with self.meta_data_path.open("r") as file:
                self.orig_meta_data = json.load(file)
            self.orig_file_name = self.orig_meta_data["orig_file_name"]
            self.df_orig = get_df_from_pq(Path(self.dir_path, self.orig_file_name))

    def get_dir_path_str(self) -> str:
        return self.dir_path.as_posix()

    def put_df_orig(
        self, df_orig: pd.DataFrame, orig_file_name: str, also_make_csv: bool = False
    ) -> None:
        if self.df_orig is not None:
            raise ValueError("df_orig is already populated.")
        if len(self.orig_meta_data) > 0:
            raise ValueError("orig_meta_data is already populated.")
        self.orig_file_name = orig_file_name + ".parquet"
        # record the number of distinct values per column

        self.orig_meta_data = {
            "pid_cols": [],
            "num_rows": df_orig.shape[0],
            "num_cols": df_orig.shape[1],
            "num_distinct_per_column": df_orig.nunique().to_dict(),
            "orig_file_name": self.orig_file_name,
            "columns": list(df_orig.columns),
            "column_dtypes": {col: str(df_orig[col].dtype) for col in df_orig.columns},
            "column_classes": best_guess_column_classification(df_orig),
        }
        # Make a dict that contains the column names and their dataframe dtypes

        self.df_orig = df_orig
        self.orig_file_path = Path(self.dir_path, self.orig_file_name)
        put_pq_from_df(self.orig_file_path, df_orig)
        if also_make_csv:
            self.orig_meta_data["orig_file_name_csv"] = self.orig_file_name + ".csv"
            put_csv_from_df(self.orig_file_path.with_suffix(".csv"), df_orig)
        self._save_meta_data()

    def set_pid_cols(self, pid_cols: list) -> None:
        self.orig_meta_data["pid_cols"] = pid_cols
        self._save_meta_data()

    def get_pid_cols(self) -> list:
        return self.orig_meta_data["pid_cols"]

    def _save_meta_data(self) -> None:
        with self.meta_data_path.open("w") as file:
            json.dump(self.orig_meta_data, file, indent=4)

    def save_sdx_stats(
        self,
        syn: Synthesizer,
        stats_file_path: Path,
        columns: list,
        elapsed_time: float,
    ) -> None:
        saver = {
            "columns": columns,
            "elapsed_time": elapsed_time,
            "orig_file_name": self.orig_file_name,
        }
        tw = TreeWalker(syn)
        saver["forest_nodes"] = tw.get_forest_nodes()
        ci = ClusterInfo(syn)
        saver["cluster_info"] = ci.get_cluster_info()
        with stats_file_path.open("w") as file:
            json.dump(saver, file, indent=4)

    def syn_file_exists(self, columns: list, do_load: bool = False) -> bool:
        data_file_name = make_data_file_name(self.orig_file_name, columns)
        file_path = Path(self.syn_dir_path, data_file_name + ".parquet")
        return file_path.exists()

    def synthesize(
        self, columns: list = None, also_save_stats: bool = False, force: bool = False
    ) -> None:
        if columns is None:
            columns = list(self.df_orig.columns)
        # remove pid columns
        columns = [col for col in columns if col not in self.orig_meta_data["pid_cols"]]
        columns.sort()
        data_file_name = make_data_file_name(self.orig_file_name, columns)
        data_file_path = Path(self.syn_dir_path, data_file_name + ".parquet")
        if data_file_path.exists() and not force:
            return
        if len(self.orig_meta_data["pid_cols"]) > 0:
            df_pid = self.df_orig[self.orig_meta_data["pid_cols"]]
        else:
            df_pid = None
        # record start of elapsed time
        start_time = time.time()
        print("df_orig")
        print(self.df_orig[columns].head())
        print(self.df_orig[columns].dtypes)
        print("df_pid")
        print(df_pid.head())
        syn = Synthesizer(self.df_orig[columns], pids=df_pid)
        df_syn = syn.sample()
        elapsed_time = time.time() - start_time
        put_pq_from_df(data_file_path, df_syn)
        if also_save_stats:
            stats_file_path = Path(self.stats_dir_path, "stats_" + data_file_name + ".json")
            self.save_sdx_stats(syn, stats_file_path, columns, elapsed_time)
            pass
