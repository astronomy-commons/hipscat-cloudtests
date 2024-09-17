"""Set of convenience methods for testing file contents"""

import re

import numpy.testing as npt
import pandas as pd
import pyarrow as pa
from hipscat.io.file_io.file_io import load_text_file
from hipscat.io.file_io.file_pointer import does_file_or_directory_exist


def assert_text_file_matches(expected_lines, file_name):
    """Convenience method to read a text file and compare the contents, line for line.

    When file contents get even a little bit big, it can be difficult to see
    the difference between an actual file and the expected contents without
    increased testing verbosity. This helper compares files line-by-line,
    using the provided strings or regular expressions.

    Notes:
        Because we check strings as regular expressions, you may need to escape some
        contents of `expected_lines`.

    Args:
        expected_lines(:obj:`string array`) list of strings, formatted as regular expressions.
        file_name (UPath): fully-specified path of the file to read
    """
    assert does_file_or_directory_exist(file_name), f"file not found [{file_name}]"
    contents = load_text_file(file_name)

    assert len(expected_lines) == len(
        contents
    ), f"files not the same length ({len(contents)} vs {len(expected_lines)})"
    for i, expected in enumerate(expected_lines):
        assert re.match(expected, contents[i]), (
            f"files do not match at line {i+1} " f"(actual: [{contents[i]}] vs expected: [{expected}])"
        )


def assert_parquet_file_ids(file_name, id_column, schema: pa.Schema, expected_ids, resort_ids=True):
    """
    Convenience method to read a parquet file and compare the object IDs to
    a list of expected objects.

    Args:
        file_name (UPath): fully-specified path of the file to read
        id_column (str): column in the parquet file to read IDs from
        expected_ids (:obj:`int[]`): list of expected ids in `id_column`
        resort_ids (bool): should we re-sort the ids? if False, we will check that the ordering
            is the same between the read IDs and expected_ids
    """
    data_frame = pd.read_parquet(file_name.path, engine="pyarrow", schema=schema, filesystem=file_name.fs)
    assert id_column in data_frame.columns
    ids = data_frame[id_column].tolist()
    if resort_ids:
        ids.sort()
        expected_ids.sort()

    assert len(ids) == len(expected_ids), f"object list not the same size ({len(ids)} vs {len(expected_ids)})"

    npt.assert_array_equal(ids, expected_ids)
