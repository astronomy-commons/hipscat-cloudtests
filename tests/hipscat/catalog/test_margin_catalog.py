from hipscat.catalog import CatalogType, MarginCatalog
from hipscat.loaders import read_from_hipscat
from hipscat.pixel_math.healpix_pixel import HealpixPixel


def test_read_margin_from_file(small_sky_margin_dir_cloud, storage_options):
    catalog = read_from_hipscat(small_sky_margin_dir_cloud, storage_options=storage_options)

    assert isinstance(catalog, MarginCatalog)
    assert catalog.on_disk
    assert catalog.catalog_path == small_sky_margin_dir_cloud
    assert len(catalog.get_healpix_pixels()) == 5
    assert catalog.get_healpix_pixels() == [
        HealpixPixel(0, 4),
        HealpixPixel(1, 44),
        HealpixPixel(1, 45),
        HealpixPixel(1, 46),
        HealpixPixel(1, 47),
    ]

    info = catalog.catalog_info
    assert info.catalog_name == "small_sky_order1_margin"
    assert info.catalog_type == CatalogType.MARGIN
    assert info.primary_catalog == "small_sky_order1"
    assert info.margin_threshold == 7200
