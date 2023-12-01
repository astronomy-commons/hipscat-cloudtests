import os

import hipscat as hc
import lsdb
import pytest
from hipscat.io.file_io import file_io

SMALL_SKY_DIR_NAME = "small_sky"
SMALL_SKY_XMATCH_NAME = "small_sky_xmatch"
SMALL_SKY_ORDER1_DIR_NAME = "small_sky_order1"
XMATCH_CORRECT_FILE = "xmatch_correct.csv"
XMATCH_CORRECT_005_FILE = "xmatch_correct_0_005.csv"
XMATCH_MOCK_FILE = "xmatch_mock.csv"


@pytest.fixture
def test_data_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "lsdb", "data")


@pytest.fixture
def small_sky_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_DIR_NAME)


@pytest.fixture
def small_sky_xmatch_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_XMATCH_NAME)


@pytest.fixture
def small_sky_order1_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_ORDER1_DIR_NAME)


@pytest.fixture
def small_sky_catalog_cloud(small_sky_dir_cloud, example_cloud_storage_options):
    return lsdb.read_hipscat(small_sky_dir_cloud, storage_options=example_cloud_storage_options)


@pytest.fixture
def small_sky_xmatch_catalog_cloud(small_sky_xmatch_dir_cloud, example_cloud_storage_options):
    return lsdb.read_hipscat(small_sky_xmatch_dir_cloud, storage_options=example_cloud_storage_options)


@pytest.fixture
def small_sky_order1_hipscat_catalog_cloud(small_sky_order1_dir_cloud, example_cloud_storage_options):
    return hc.catalog.Catalog.read_from_hipscat(
        small_sky_order1_dir_cloud, storage_options=example_cloud_storage_options
    )


@pytest.fixture
def small_sky_order1_catalog_cloud(small_sky_order1_dir_cloud, example_cloud_storage_options):
    return lsdb.read_hipscat(small_sky_order1_dir_cloud, storage_options=example_cloud_storage_options)


@pytest.fixture
def xmatch_correct_cloud(small_sky_xmatch_dir_cloud, example_cloud_storage_options):
    pathway = os.path.join(small_sky_xmatch_dir_cloud, XMATCH_CORRECT_FILE)
    return file_io.load_csv_to_pandas(pathway, storage_options=example_cloud_storage_options)
