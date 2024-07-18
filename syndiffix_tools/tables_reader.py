import json
import os
import time
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from syndiffix.synthesizer import Synthesizer

from syndiffix_tools.cluster_info import *
from syndiffix_tools.common_tasks import *
from syndiffix_tools.tree_walker import *


class TablesReader:
    """
    This class takes the synthetic datasets and metadata generated by TablesManager
    and provides methods to read them.

    We assume that the synthetic datasets do not change once this class in invoked.
    If they do change, then a new class must be invoked with the updated set.

    Inputs:
        - dir_path: str or Path. the directory path where the synthetic
              datasets and other metadata are stored.
        - cache: bool. If True, the synthetic datasets are cached in memory
              as they are retrieved.
    """

    def __init__(self, dir_path: Union[str, Path], cache: bool = False) -> None:
        if type(dir_path) == str:
            self.syn_dir_path = Path(dir_path)
        else:
            self.syn_dir_path = dir_path
        if not self.syn_dir_path.exists():
            raise FileNotFoundError(f"Directory {self.syn_dir_path} does not exist.")
        self.cache = cache
        self.catalog = None
        self.all_columns = []
        self._build_catalog()

    def _build_catalog(self) -> None:
        self.catalog = []
        for meta_data_path in self.syn_dir_path.iterdir():
            if meta_data_path.suffix == ".json":
                with meta_data_path.open("r") as file:
                    meta_data = json.load(file)
                dataset_path = meta_data_path.with_suffix(".parquet")
                # convert dataset_path to a string
                if not dataset_path.exists():
                    raise FileNotFoundError(f"Dataset file {dataset_path.as_posix()} does not exist.")
                meta_data['dataset_path'] = dataset_path
                meta_data['df'] = None
                if len(meta_data["columns"]) > len(self.all_columns):
                    self.all_columns = meta_data["columns"]
                self.catalog.append(meta_data)

    def get_best_syn_df(self, columns: list = None, target: str = None) -> Optional[pd.DataFrame]:
        if columns is None:
            columns = self.all_columns
        best_match_columns = None
        best_match_entry = None
        for entry in self.catalog:
            if target is not None and entry["target_column"] != target:
                continue
            entry_columns = entry["columns"]
            if all(col in entry_columns for col in columns):
                if best_match_columns is None or len(entry_columns) < len(best_match_columns):
                    best_match_columns = entry_columns
                    best_match_entry = entry
        if best_match_entry is not None:
            if best_match_entry['df'] is not None:
                return best_match_entry['df']
            best_match_df = pd.read_parquet(best_match_entry['dataset_path'])
            if self.cache:
                best_match_entry['df'] = best_match_df
            return best_match_df
        else:
            return None