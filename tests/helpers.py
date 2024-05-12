import numpy as np
import pandas as pd


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


def get_generic_dataframe_big():
    np.random.seed(0)
    n = 100
    df = pd.DataFrame(
        {
            "str5": np.random.choice(list("abcde"), n),
            "int10": np.random.randint(1, 11, n),
            "float": np.random.randn(n),
            "datetime": pd.date_range("2000-01-01", "2020-01-01", periods=n),
            "str5a": np.random.choice(list("fghij"), n),
            "int10a": np.random.randint(20, 31, n),
            "floata": np.random.randn(n),
            "datetimea": pd.date_range("1980-01-01", "2000-01-01", periods=n),
            "str5b": np.random.choice(list("klmno"), n),
            "int10b": np.random.randint(30, 41, n),
            "floatb": np.random.randn(n),
            "datetimeb": pd.date_range("1960-01-01", "1980-01-01", periods=n),
        }
    )
    df["pid"] = df.index
    return df
