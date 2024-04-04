from common_tasks import *


def test_python_version():
    import sys

    print(sys.version)


def test_make_data_file_name():
    assert (
        make_data_file_name("table1", ["col1", "col2"])
        == "sdx.table1.col2.col1_col2.5s8aey"
    )
    # reversing column names gives the same result
    assert (
        make_data_file_name("table1", ["col2", "col1"])
        == "sdx.table1.col2.col1_col2.5s8aey"
    )
    # make columns names with spaces
    assert (
        make_data_file_name("table1", ["col 1", "col 2"])
        == "sdx.table1.col2.col_1_col_2.gvb6zg"
    )
    # make three columns names of at least 20 characters
    assert (
        make_data_file_name("table1", ["col 1" * 4, "col 2" * 4, "col 3" * 4])
        == "sdx.table1.col3.col_1col_1c_col_2col_2c_col_3col_3c.xz28ge"
    )
    # make 40 columns
    assert (
        make_data_file_name("table1", ["col" + str(i) for i in range(40)])
        == "sdx.table1.col40..cdesig"
    )
