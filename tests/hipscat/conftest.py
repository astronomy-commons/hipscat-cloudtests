import os
import os.path

import pytest

# pylint: disable=missing-function-docstring, redefined-outer-name


@pytest.fixture
def base_catalog_info_file_cloud(test_data_dir_cloud) -> str:
    return os.path.join(test_data_dir_cloud, "dataset", "catalog_info.json")
