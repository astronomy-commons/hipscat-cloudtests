import os

from hipscat.inspection.almanac import Almanac

# def test_default(almanac_dir_cloud, test_data_dir_cloud, file_system, storage_options):
#     """Test loading from a default directory"""

#     os.environ["HIPSCAT_ALMANAC_DIR"] = ""
#     os.environ["HIPSCAT_DEFAULT_DIR"] = test_data_dir_cloud

#     alms = Almanac(include_default_dir=True, file_system=file_system,storage_options=storage_options)
#     assert len(alms.catalogs()) == 0

#     os.environ["HIPSCAT_ALMANAC_DIR"] = almanac_dir_cloud
#     alms = Almanac(include_default_dir=True, file_system=file_system,storage_options=storage_options)
#     assert len(alms.catalogs()) == 2

#     os.environ.pop("HIPSCAT_ALMANAC_DIR")
#     alms = Almanac(include_default_dir=True, file_system=file_system,storage_options=storage_options)
#     assert len(alms.catalogs()) == 0
