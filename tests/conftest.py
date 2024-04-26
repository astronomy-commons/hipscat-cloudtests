import os

import hipscat as hc
import lsdb
import pytest
import shortuuid
from hipscat.io.file_io import file_io

from hipscat_cloudtests.temp_cloud_directory import TempCloudDirectory

SMALL_SKY_XMATCH_NAME = "small_sky_xmatch"
XMATCH_CORRECT_FILE = "xmatch_correct.csv"

ALMANAC_DIR_NAME = "almanac"
SMALL_SKY_DIR_NAME = "small_sky"
SMALL_SKY_ORDER1_DIR_NAME = "small_sky_order1"


TEST_DIR = os.path.dirname(__file__)
SMALL_SKY_DIR_NAME = "small_sky"


def pytest_addoption(parser):
    parser.addoption("--cloud", action="store", default="abfs")


@pytest.fixture(scope="session", name="cloud")
def cloud(request):
    return request.config.getoption("--cloud")


@pytest.fixture(scope="session", name="cloud_path")
def cloud_path(cloud):
    if cloud == "abfs":
        return "abfs://hipscat/pytests/"

    raise NotImplementedError("Cloud format not implemented for tests!")


@pytest.fixture(scope="session", name="storage_options")
def storage_options(cloud):
    if cloud == "abfs":
        storage_options = {
            "account_key": os.environ.get("ABFS_LINCCDATA_ACCOUNT_KEY"),
            "account_name": os.environ.get("ABFS_LINCCDATA_ACCOUNT_NAME"),
        }
        return storage_options

    return {}


@pytest.fixture
def local_data_dir():
    local_data_path = os.path.dirname(__file__)
    return os.path.join(local_data_path, "data")


@pytest.fixture
def small_sky_dir_local(local_data_dir):
    return os.path.join(local_data_dir, SMALL_SKY_DIR_NAME)


@pytest.fixture
def small_sky_order1_dir_local(local_data_dir):
    return os.path.join(local_data_dir, SMALL_SKY_ORDER1_DIR_NAME)


@pytest.fixture
def small_sky_parts_dir_local(local_data_dir):
    return os.path.join(local_data_dir, "small_sky_parts")


@pytest.fixture
def test_data_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data")


@pytest.fixture
def almanac_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", ALMANAC_DIR_NAME)


@pytest.fixture
def small_sky_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", SMALL_SKY_DIR_NAME)


@pytest.fixture
def small_sky_order1_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", SMALL_SKY_ORDER1_DIR_NAME)


@pytest.fixture
def small_sky_index_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", "small_sky_object_index")


@pytest.fixture
def small_sky_margin_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", "small_sky_order1_margin")


@pytest.fixture(scope="session", name="tmp_dir_cloud")
def tmp_dir_cloud(cloud_path, storage_options):
    """Create a single client for use by all unit test cases."""
    tmp = TempCloudDirectory(
        os.path.join(cloud_path, "tmp"),
        method_name="full_test",
        storage_options=storage_options,
    )
    yield tmp.open()
    tmp.close()


@pytest.fixture
def tmp_cloud_path(request, tmp_dir_cloud):
    name = request.node.name
    my_uuid = shortuuid.uuid()
    # Strip out the "test_" at the beginning of each method name, make it a little
    # shorter, and add a disambuating UUID.
    return f"{tmp_dir_cloud}/{name[5:25]}_{my_uuid}"


@pytest.fixture
def small_sky_xmatch_dir_cloud(cloud_path):
    return os.path.join(cloud_path, "data", SMALL_SKY_XMATCH_NAME)


@pytest.fixture
def small_sky_catalog_cloud(small_sky_dir_cloud, storage_options):
    return lsdb.read_hipscat(small_sky_dir_cloud, storage_options=storage_options)


@pytest.fixture
def small_sky_xmatch_catalog_cloud(small_sky_xmatch_dir_cloud, storage_options):
    return lsdb.read_hipscat(small_sky_xmatch_dir_cloud, storage_options=storage_options)


@pytest.fixture
def small_sky_order1_hipscat_catalog_cloud(small_sky_order1_dir_cloud, storage_options):
    return hc.catalog.Catalog.read_from_hipscat(small_sky_order1_dir_cloud, storage_options=storage_options)


@pytest.fixture
def small_sky_order1_catalog_cloud(small_sky_order1_dir_cloud, storage_options):
    return lsdb.read_hipscat(small_sky_order1_dir_cloud, storage_options=storage_options)


@pytest.fixture
def xmatch_correct_cloud(local_data_dir):
    pathway = os.path.join(local_data_dir, "xmatch", XMATCH_CORRECT_FILE)
    return file_io.load_csv_to_pandas(pathway)


@pytest.fixture
def xmatch_with_margin(local_data_dir):
    pathway = os.path.join(local_data_dir, "xmatch", "xmatch_with_margin.csv")
    return file_io.load_csv_to_pandas(pathway)
