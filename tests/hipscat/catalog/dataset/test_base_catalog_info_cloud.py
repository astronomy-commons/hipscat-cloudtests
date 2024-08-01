import dataclasses

from hipscat.catalog.dataset.base_catalog_info import BaseCatalogInfo
from hipscat.io import file_io


def test_read_from_file(base_catalog_info_file_cloud, file_system, storage_options):
    base_cat_info_fp = file_io.get_file_pointer_from_path(base_catalog_info_file_cloud)
    catalog_info = BaseCatalogInfo.read_from_metadata_file(
        base_cat_info_fp, file_system=file_system, storage_options=storage_options
    )
    catalog_info_json = file_io.file_io.load_json_file(
        base_catalog_info_file_cloud,
        file_system=file_system,
        storage_options=storage_options,
    )

    catalog_info_dict = dataclasses.asdict(catalog_info)
    for key, value in catalog_info_json.items():
        assert catalog_info_dict[key] == value
