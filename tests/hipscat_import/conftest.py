import os
import os.path

import pytest
from dask.distributed import Client


@pytest.fixture(scope="session", name="dask_client")
def dask_client():
    """Create a single client for use by all unit test cases."""
    client = Client()
    yield client
    client.close()


@pytest.fixture
def small_sky_parts_dir_cloud(example_cloud_path):
    return os.path.join(example_cloud_path, "hipscat_import", "data", "small_sky_parts")


@pytest.fixture
def small_sky_parts_dir_local(local_data_dir):
    return os.path.join(local_data_dir, "small_sky_parts")
