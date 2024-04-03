import os

import lsdb
import pandas as pd


def test_save_catalog_and_margin(local_data_dir, storage_options, tmp_cloud_path):
    pathway = os.path.join(local_data_dir, "xmatch", "xmatch_catalog_raw.csv")
    input_df = pd.read_csv(pathway)
    catalog = lsdb.from_dataframe(
        input_df, margin_threshold=5000, catalog_name="small_sky_from_dataframe", catalog_type="object"
    )

    base_catalog_path = f"{tmp_cloud_path}/new_catalog_name"
    catalog.to_hipscat(base_catalog_path, storage_options=storage_options)
    expected_catalog = lsdb.read_hipscat(base_catalog_path, storage_options=storage_options)
    assert expected_catalog.hc_structure.catalog_name == catalog.hc_structure.catalog_name
    assert expected_catalog.hc_structure.catalog_info == catalog.hc_structure.catalog_info
    assert expected_catalog.get_healpix_pixels() == catalog.get_healpix_pixels()
    pd.testing.assert_frame_equal(expected_catalog.compute(), catalog._ddf.compute())

    base_catalog_path = f"{tmp_cloud_path}/new_margin_name"
    catalog.margin.to_hipscat(base_catalog_path, storage_options=storage_options)
    expected_catalog = lsdb.read_hipscat(base_catalog_path, storage_options=storage_options)
    assert expected_catalog.hc_structure.catalog_name == catalog.margin.hc_structure.catalog_name
    assert expected_catalog.hc_structure.catalog_info == catalog.margin.hc_structure.catalog_info
    assert expected_catalog.get_healpix_pixels() == catalog.margin.get_healpix_pixels()
    pd.testing.assert_frame_equal(expected_catalog.compute(), catalog.margin._ddf.compute())
