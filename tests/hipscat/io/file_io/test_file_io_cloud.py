import os

import numpy as np
import pandas as pd
from hipscat.io.file_io import (
    get_file_pointer_from_path,
    load_csv_to_pandas,
    load_json_file,
    load_parquet_to_pandas,
    load_text_file,
    write_dataframe_to_csv,
    write_string_to_file,
)
from hipscat.io.paths import pixel_catalog_file

from hipscat_cloudtests import TempCloudDirectory


def test_write_string_to_file(tmp_dir_cloud, example_cloud_storage_options):
    with TempCloudDirectory(tmp_dir_cloud, "write_string", example_cloud_storage_options) as temp_path:
        test_file_path = os.path.join(temp_path, "text_file.txt")
        test_file_pointer = get_file_pointer_from_path(test_file_path)
        test_string = "this is a test"
        write_string_to_file(
            test_file_pointer,
            test_string,
            encoding="utf-8",
            storage_options=example_cloud_storage_options,
        )
        data = load_text_file(test_file_path, encoding="utf-8", storage_options=example_cloud_storage_options)
        assert data[0] == test_string


def test_load_json(small_sky_dir_local, small_sky_dir_cloud, example_cloud_storage_options):
    catalog_cloud_path = os.path.join(small_sky_dir_cloud, "catalog_info.json")
    catalog_info_path = os.path.join(small_sky_dir_local, "catalog_info.json")
    catalog_info_pointer = get_file_pointer_from_path(catalog_info_path)
    json_dict_cloud = load_json_file(catalog_cloud_path, storage_options=example_cloud_storage_options)
    json_dict_local = load_json_file(catalog_info_pointer, encoding="utf-8")
    assert json_dict_cloud == json_dict_local


def test_load_parquet_to_pandas(small_sky_dir_local, small_sky_dir_cloud, example_cloud_storage_options):
    pixel_data_path = pixel_catalog_file(small_sky_dir_local, 0, 11)
    pixel_data_path_cloud = pixel_catalog_file(small_sky_dir_cloud, 0, 11)
    parquet_df = pd.read_parquet(pixel_data_path)
    loaded_df = load_parquet_to_pandas(pixel_data_path_cloud, storage_options=example_cloud_storage_options)
    pd.testing.assert_frame_equal(parquet_df, loaded_df)


def test_write_df_to_csv(tmp_dir_cloud, example_cloud_storage_options):
    with TempCloudDirectory(tmp_dir_cloud, "write_df_to_csv", example_cloud_storage_options) as temp_path:
        random_df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
        test_file_path = os.path.join(temp_path, "test.csv")
        test_file_pointer = get_file_pointer_from_path(test_file_path)
        write_dataframe_to_csv(
            random_df,
            test_file_pointer,
            index=False,
            storage_options=example_cloud_storage_options,
        )
        loaded_df = load_csv_to_pandas(test_file_pointer, storage_options=example_cloud_storage_options)
        pd.testing.assert_frame_equal(loaded_df, random_df)
