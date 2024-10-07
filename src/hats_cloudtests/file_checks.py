"""Set of convenience methods for testing file contents"""

import numpy.testing as npt
import pandas as pd
import pyarrow as pa


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
