import os

import pytest

DATA_DIR_NAME = "data"

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
