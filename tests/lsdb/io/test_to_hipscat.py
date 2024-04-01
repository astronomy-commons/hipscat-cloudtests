import os

import lsdb
import pandas as pd

from hipscat_cloudtests import TempCloudDirectory


def test_save_catalog_and_margin(local_data_dir, example_cloud_storage_options, tmp_dir_cloud):
    pathway = os.path.join(local_data_dir, "xmatch", "xmatch_catalog_raw.csv")
    input_df = pd.read_csv(pathway)
    catalog = lsdb.from_dataframe(
        input_df, margin_threshold=5000, catalog_name="small_sky_from_dataframe", catalog_type="object"
    )

    with TempCloudDirectory(
        tmp_dir_cloud, "lsdb_save_catalog_and_margin", example_cloud_storage_options
    ) as temp_path:
        base_catalog_path = f"{temp_path}/new_catalog_name"
        catalog.to_hipscat(base_catalog_path, storage_options=example_cloud_storage_options)
        expected_catalog = lsdb.read_hipscat(base_catalog_path, storage_options=example_cloud_storage_options)
        assert expected_catalog.hc_structure.catalog_name == catalog.hc_structure.catalog_name
        assert expected_catalog.hc_structure.catalog_info == catalog.hc_structure.catalog_info
        assert expected_catalog.get_healpix_pixels() == catalog.get_healpix_pixels()
        pd.testing.assert_frame_equal(expected_catalog.compute(), catalog._ddf.compute())

        base_catalog_path = f"{temp_path}/new_margin_name"
        catalog.margin.to_hipscat(base_catalog_path, storage_options=example_cloud_storage_options)
        expected_catalog = lsdb.read_hipscat(base_catalog_path, storage_options=example_cloud_storage_options)
        assert expected_catalog.hc_structure.catalog_name == catalog.margin.hc_structure.catalog_name
        assert expected_catalog.hc_structure.catalog_info == catalog.margin.hc_structure.catalog_info
        assert expected_catalog.get_healpix_pixels() == catalog.margin.get_healpix_pixels()
        pd.testing.assert_frame_equal(expected_catalog.compute(), catalog.margin._ddf.compute())
