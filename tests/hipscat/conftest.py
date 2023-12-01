import os
import os.path

import pytest

ALMANAC_DIR_NAME = "almanac"
SMALL_SKY_DIR_NAME = "small_sky"
SMALL_SKY_ORDER1_DIR_NAME = "small_sky_order1"
SMALL_SKY_TO_SMALL_SKY_ORDER1_DIR_NAME = "small_sky_to_small_sky_order1"

# pylint: disable=missing-function-docstring, redefined-outer-name


@pytest.fixture
def tmp_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "hipscat", "tmp")


@pytest.fixture
def test_data_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "hipscat", "data")


@pytest.fixture
def almanac_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, ALMANAC_DIR_NAME)


@pytest.fixture
def small_sky_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_DIR_NAME)


@pytest.fixture
def small_sky_order1_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_ORDER1_DIR_NAME)


@pytest.fixture
def base_catalog_info_file_cloud(test_data_dir_cloud) -> str:
    return os.path.join(test_data_dir_cloud, "dataset", "catalog_info.json")


@pytest.fixture
def catalog_info_file_cloud(catalog_path_cloud) -> str:
    return os.path.join(catalog_path_cloud, "catalog_info.json")
