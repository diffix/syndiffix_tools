import json
import time
from pathlib import Path
from typing import Union, Optional

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
        self.catalog = None

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

    def build_catalog(self, cache: bool = False) -> None:
        self.catalog = []
        for file_path in self.syn_dir_path.iterdir():
            if file_path.suffix == ".parquet":
                df = get_df_from_pq(file_path)
                columns = list(df.columns)
                cat_entry = {"file_path": file_path, "columns": columns}
                if cache:
                    cat_entry["df"] = df
                else:
                    cat_entry["df"] = None
                self.catalog.append(cat_entry)


    def get_best_syn_df(self, columns: list = None, cache: bool = False) -> Optional[pd.DataFrame]:
        if columns is None:
            columns = list(self.df_orig.columns)
        if self.catalog is None:
            self.build_catalog(cache=cache)
        best_match_columns = None
        for entry in self.catalog:
            entry_columns = entry["columns"]
            if all(col in entry_columns for col in columns):
                if best_match_columns is None or len(entry_columns) < len(best_match_columns):
                    best_match_columns = entry_columns
        if best_match_columns is not None:
            best_match_df = self.get_syn_df(best_match_columns)
            return best_match_df
        else:
            return None

    def _build_meta_data(self,
                         syn: Synthesizer,
                         df_syn: pd.DataFrame,
                         elapsed_time: float,
                         target_column: str = None,
                         ) -> dict:
        meta_data = {
            "columns": list(df_syn.columns),
            "rows": df_syn.shape[0],
            "target_column": target_column,
            "elapsed_time": elapsed_time,
            "cluster_info": None,
        }
        ci = ClusterInfo(syn)
        meta_data["cluster_info"] = ci.get_cluster_info()
        return meta_data

    def _save_sdx_stats(
        self,
        syn: Synthesizer,
        stats_file_path: Path,
        columns: list,
        elapsed_time: float,
        save_stats: str,
        target_column: str = None,
    ) -> None:
        if save_stats == 'none':
            return
        saver = {
            "columns": columns,
            "target_column": target_column,
            "elapsed_time": elapsed_time,
            "orig_file_name": self.orig_file_name,
            "forest_nodes": None,
            "cluster_info": None,
        }
        ci = ClusterInfo(syn)
        saver["cluster_info"] = ci.get_cluster_info()
        if save_stats == 'max':
            tw = TreeWalker(syn)
            saver["forest_nodes"] = tw.get_forest_nodes()
        with stats_file_path.open("w") as file:
            json.dump(saver, file, indent=4)

    def syn_file_exists(self, columns: list, target_column: str = None) -> bool:
        data_file_name = make_data_file_name(self.orig_file_name, columns, target=target_column)
        file_path = Path(self.syn_dir_path, data_file_name + ".parquet")
        return file_path.exists()

    def get_syn_df(self, columns: list = None, target_column: str = None) -> Optional[pd.DataFrame]:
        if columns is None:
            columns = list(self.df_orig.columns)
        data_file_name = make_data_file_name(self.orig_file_name, columns, target=target_column)
        file_path = Path(self.syn_dir_path, data_file_name + ".parquet")
        if file_path.exists():
            return pd.read_parquet(file_path)
        else:
            return None

    def synthesize(
        self, 
        columns: list = None, 
        target_column: str = None,
        save_stats: str = 'min', 
        force: bool = False,
        also_save_stats: bool = None,     # deprecated
    ) -> None:
        ''' columns: list of column names to synthesize. If None, all
               columns are synthesized.
            target_column: use as the ML target column
            save_stats: 'min', 'max', or 'none'. 'max' can be quite large.
            force: if True, synthesize even if the file already exists.
        '''
        # also_save_stats is deprecated
        if also_save_stats is not None:
            if also_save_stats is True:
                save_stats = 'max'
            else:
                save_stats = 'none'
        if columns is None:
            columns = list(self.df_orig.columns)
        # remove pid columns
        columns = [col for col in columns if col not in self.orig_meta_data["pid_cols"]]
        columns.sort()
        data_file_name = make_data_file_name(self.orig_file_name, columns, target=target_column)
        data_file_path = Path(self.syn_dir_path, data_file_name + ".parquet")
        if data_file_path.exists() and not force:
            return
        if len(self.orig_meta_data["pid_cols"]) > 0:
            df_pid = self.df_orig[self.orig_meta_data["pid_cols"]]
        else:
            df_pid = None
        # record start of elapsed time
        start_time = time.time()
        syn = Synthesizer(self.df_orig[columns], pids=df_pid, target_column=target_column)
        df_syn = syn.sample()
        elapsed_time = time.time() - start_time
        put_pq_from_df(data_file_path, df_syn)
        meta_data = self._build_meta_data(syn, df_syn, elapsed_time, target_column=target_column)
        meta_data_path = Path(self.syn_dir_path, data_file_name + ".meta_data.json")
        with meta_data_path.open("w") as file:
            json.dump(meta_data, file, indent=4)
        # The catalog would be out of date after this, so we just delete it
        # and rebuild it when needed
        self.catalog = None
        if save_stats != 'none':
            stats_file_path = Path(self.stats_dir_path, "stats_" + data_file_name + ".json")
            self._save_sdx_stats(syn, stats_file_path, columns, elapsed_time, save_stats, target_column=target_column)