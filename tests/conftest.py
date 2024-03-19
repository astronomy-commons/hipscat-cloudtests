import os

import pytest

ALMANAC_DIR_NAME = "almanac"
SMALL_SKY_DIR_NAME = "small_sky"
SMALL_SKY_ORDER1_DIR_NAME = "small_sky_order1"


TEST_DIR = os.path.dirname(__file__)
SMALL_SKY_DIR_NAME = "small_sky"


def pytest_addoption(parser):
    parser.addoption("--cloud", action="store", default="abfs")


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.cloud
    if "cloud" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("cloud", [option_value])


@pytest.fixture
def example_cloud_path(cloud):
    if cloud == "abfs":
        return "abfs://hipscat/pytests/"

    raise NotImplementedError("Cloud format not implemented for tests!")


@pytest.fixture
def example_cloud_storage_options(cloud):
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
def tmp_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "tmp")


@pytest.fixture
def test_data_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "data")


@pytest.fixture
def almanac_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, ALMANAC_DIR_NAME)


@pytest.fixture
def small_sky_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_DIR_NAME)


@pytest.fixture
def small_sky_order1_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, SMALL_SKY_ORDER1_DIR_NAME)
