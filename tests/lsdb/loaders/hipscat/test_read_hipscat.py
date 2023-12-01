import lsdb


def test_read_hipscat(
    small_sky_order1_dir_cloud,
    small_sky_order1_hipscat_catalog_cloud,
    example_cloud_storage_options,
):
    catalog = lsdb.read_hipscat(small_sky_order1_dir_cloud, storage_options=example_cloud_storage_options)
    assert isinstance(catalog, lsdb.Catalog)
    assert catalog.hc_structure.catalog_base_dir == small_sky_order1_hipscat_catalog_cloud.catalog_base_dir
    assert catalog.get_healpix_pixels() == small_sky_order1_hipscat_catalog_cloud.get_healpix_pixels()

    catalog = lsdb.read_hipscat(small_sky_order1_dir_cloud, storage_options=example_cloud_storage_options)
    for healpix_pixel in small_sky_order1_hipscat_catalog_cloud.get_healpix_pixels():
        catalog.get_partition(healpix_pixel.order, healpix_pixel.pixel)
