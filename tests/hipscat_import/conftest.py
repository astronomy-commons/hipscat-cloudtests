import os
import os.path

import pytest
from dask.distributed import Client

DATA_DIR_NAME = "data"
SMALL_SKY_DIR_NAME = "small_sky"


@pytest.fixture(scope="session", name="dask_client")
def dask_client():
    """Create a single client for use by all unit test cases."""
    client = Client()
    yield client
    client.close()


def pytest_collection_modifyitems(items):
    """Modify dask unit tests to
        - ignore event loop deprecation warnings
        - have a longer timeout default timeout (5 seconds instead of 1 second)
        - require use of the `dask_client` fixture, even if it's not requested

    Individual tests that will be particularly long-running can still override
    the default timeout, by using an annotation like:

        @pytest.mark.dask(timeout=10)
        def test_long_running():
            ...
    """
    first_dask = True
    for item in items:
        timeout = None
        for mark in item.iter_markers(name="dask"):
            timeout = 15
            if "timeout" in mark.kwargs:
                timeout = int(mark.kwargs.get("timeout"))
        if timeout:
            if first_dask:
                ## The first test requires more time to set up the dask/ray client
                timeout += 10
                first_dask = False
            item.add_marker(pytest.mark.timeout(timeout))
            item.add_marker(pytest.mark.usefixtures("dask_client"))
            item.add_marker(pytest.mark.filterwarnings("ignore::DeprecationWarning"))


@pytest.fixture
def tmp_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "hipscat_import", "tmp")


@pytest.fixture
def test_data_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "hipscat_import", DATA_DIR_NAME)


@pytest.fixture
def small_sky_parts_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, "small_sky_parts")


@pytest.fixture
def small_sky_parts_dir_local(local_data_dir):
    return os.path.join(local_data_dir, "small_sky_parts")


@pytest.fixture
def small_sky_catalog_dir_cloud(test_data_dir_cloud):
    return os.path.join(test_data_dir_cloud, "small_sky")
