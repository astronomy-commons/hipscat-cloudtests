import os

import hipscat as hc
import lsdb
import pytest
from hipscat.io.file_io import file_io

SMALL_SKY_XMATCH_NAME = "small_sky_xmatch"
XMATCH_CORRECT_FILE = "xmatch_correct.csv"


@pytest.fixture
def small_sky_xmatch_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "data", SMALL_SKY_XMATCH_NAME)


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
def xmatch_correct_cloud(local_data_dir):
    pathway = os.path.join(local_data_dir, "xmatch", XMATCH_CORRECT_FILE)
    return file_io.load_csv_to_pandas(pathway)


@pytest.fixture
def xmatch_with_margin(local_data_dir):
    pathway = os.path.join(local_data_dir, "xmatch", "xmatch_with_margin.csv")
    return file_io.load_csv_to_pandas(pathway)
