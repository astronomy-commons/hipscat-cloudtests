import numpy.testing as npt
from hipscat.catalog.index.index_catalog import IndexCatalog
from hipscat.loaders import read_from_hipscat
from hipscat.pixel_math import HealpixPixel


def test_loc_partition(small_sky_index_dir_cloud, example_cloud_storage_options):
    catalog = read_from_hipscat(small_sky_index_dir_cloud, storage_options=example_cloud_storage_options)

    assert isinstance(catalog, IndexCatalog)
    assert catalog.on_disk
    assert catalog.catalog_path == small_sky_index_dir_cloud

    npt.assert_array_equal(catalog.loc_partitions([700]), [HealpixPixel(1, 46)])
    npt.assert_array_equal(catalog.loc_partitions([707]), [HealpixPixel(1, 44)])
    npt.assert_array_equal(catalog.loc_partitions([900]), [])
