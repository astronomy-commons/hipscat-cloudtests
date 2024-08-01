import lsdb
from hipscat.pixel_math import HealpixPixel
from lsdb.catalog.margin_catalog import MarginCatalog


def test_read_hipscat(
    small_sky_order1_dir_cloud,
    small_sky_order1_hipscat_catalog_cloud,
    storage_options,
):
    catalog = lsdb.read_hipscat(
        small_sky_order1_dir_cloud,
        file_system=file_system,
        storage_options=storage_options,
    )
    assert isinstance(catalog, lsdb.Catalog)
    assert catalog.hc_structure.catalog_base_dir == small_sky_order1_hipscat_catalog_cloud.catalog_base_dir
    assert catalog.get_healpix_pixels() == small_sky_order1_hipscat_catalog_cloud.get_healpix_pixels()

    for healpix_pixel in small_sky_order1_hipscat_catalog_cloud.get_healpix_pixels():
        catalog.get_partition(healpix_pixel.order, healpix_pixel.pixel)


def test_read_hipscat_margin(
    small_sky_margin_dir_cloud,
    storage_options,
):
    catalog = lsdb.read_hipscat(
        small_sky_margin_dir_cloud,
        file_system=file_system,
        storage_options=storage_options,
    )
    assert isinstance(catalog, MarginCatalog)
    assert catalog.hc_structure.catalog_base_dir == small_sky_margin_dir_cloud
    assert catalog.get_healpix_pixels() == [
        HealpixPixel(0, 4),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]
