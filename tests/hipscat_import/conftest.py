import pytest
from dask.distributed import Client


@pytest.fixture(scope="session", name="dask_client")
def dask_client():
    """Create a single client for use by all unit test cases."""
    client = Client()
    yield client
    client.close()


@pytest.fixture
def small_sky_parts_dir_cloud(cloud_path):
    return cloud_path / "hipscat_import" / "data" / "small_sky_parts"
