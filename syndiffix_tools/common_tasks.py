import random
import string
from pathlib import Path

import pandas as pd


# Utility function for loading a CSV file.
def get_df_from_csv(path: Path) -> pd.DataFrame:
    from pandas.errors import ParserError

    df = pd.read_csv(path, keep_default_na=False, na_values=[""], low_memory=False)
    # Try to infer datetime columns.
    for col in df.columns[df.dtypes == "object"]:
        try:
            df[col] = pd.to_datetime(df[col], format="ISO8601")
        except (ParserError, ValueError):
            pass
    return df


def put_csv_from_df(filePath: Path, df: pd.DataFrame) -> None:
    # Save to CSV file
    df.to_csv(filePath, index=False)


def put_pq_from_df(filePath: Path, df: pd.DataFrame) -> None:
    # Save to Parquet file
    df.to_parquet(filePath, engine="pyarrow")


def get_df_from_pq(filePath: Path) -> pd.DataFrame:
    # Load from Parquet file
    df = pd.read_parquet(filePath, engine="pyarrow")
    return df


def best_guess_column_classification(df: pd.DataFrame) -> dict:
    # This function takes a dataframe and returns a dictionary with the best guess as to whether each column is continuous or categorical.
    # loop through the columns and associated dtypes
    col_types = {}
    for col in df.columns:
        if df[col].dtype == "float64":
            col_type = "continuous"
        elif df[col].dtype == "datetime64[ns]":
            col_type = "continuous"
        elif df[col].dtype == "int64":
            if len(df[col].unique()) < 20:
                col_type = "categorical"
            else:
                col_type = "continuous"
        else:
            col_type = "categorical"
        col_types[col] = col_type
    return col_types


def make_data_file_name(table: str, columns: list[str]) -> str:
    if table[-8:] == ".parquet":
        table = table[:-8]
    elif table[-4:] == ".csv":
        table = table[:-4]
    columns.sort()
    name = "sdx." + table + f".col{len(columns)}."
    if len(columns) <= 10:
        chars_per_col = int(30 / len(columns)) + 1
        for col in columns:
            # substitute whitespace with underscore
            col = col.replace(" ", "_")
            name += col[:chars_per_col] + "_"
        # strip off the last underscore
        name = name[:-1]
    # set a seed value to be the concatination of table and all the columns
    seed = table + "__".join(columns)
    random.seed(seed)
    # append random alphanumeric characters
    name += "." + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return name
