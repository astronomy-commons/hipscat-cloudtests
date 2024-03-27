from hipscat.catalog.index.index_catalog import IndexCatalog


def test_index_search(
    small_sky_order1_catalog_cloud,
    small_sky_index_dir_cloud,
    example_cloud_storage_options,
):
    catalog_index = IndexCatalog.read_from_hipscat(
        small_sky_index_dir_cloud, storage_options=example_cloud_storage_options
    )

    index_search_catalog = small_sky_order1_catalog_cloud.index_search([900], catalog_index)
    index_search_df = index_search_catalog.compute()
    assert len(index_search_df) == 0

    index_search_catalog = small_sky_order1_catalog_cloud.index_search([700], catalog_index)
    index_search_df = index_search_catalog.compute()
    assert len(index_search_df) == 1
