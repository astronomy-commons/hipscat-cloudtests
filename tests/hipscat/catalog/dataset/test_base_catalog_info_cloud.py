import dataclasses

from hipscat.catalog.dataset.base_catalog_info import BaseCatalogInfo
from hipscat.io import file_io


def test_read_from_file(test_data_dir_cloud):
    base_catalog_info_file_cloud = test_data_dir_cloud / "dataset" / "catalog_info.json"
    catalog_info = BaseCatalogInfo.read_from_metadata_file(base_catalog_info_file_cloud)
    catalog_info_json = file_io.file_io.load_json_file(base_catalog_info_file_cloud)

    catalog_info_dict = dataclasses.asdict(catalog_info)
    for key, value in catalog_info_json.items():
        assert catalog_info_dict[key] == value
