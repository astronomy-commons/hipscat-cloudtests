"""Tests of catalog functionality"""

import pytest
from hipscat.catalog import Catalog, PartitionInfo
from hipscat.io.file_io import file_io
from hipscat.io.validation import is_valid_catalog
from hipscat.loaders import read_from_hipscat
from hipscat.pixel_math import HealpixPixel


def test_load_catalog_small_sky(small_sky_dir_cloud):
    """Instantiate a catalog with 1 pixel"""
    cat = Catalog.read_from_hipscat(small_sky_dir_cloud)

    assert cat.catalog_name == "small_sky"
    assert len(cat.get_healpix_pixels()) == 1

    assert is_valid_catalog(small_sky_dir_cloud)


def test_load_catalog_small_sky_with_loader(small_sky_dir_cloud):
    """Instantiate a catalog with 1 pixel"""
    cat = read_from_hipscat(small_sky_dir_cloud)

    assert isinstance(cat, Catalog)
    assert cat.catalog_name == "small_sky"
    assert len(cat.get_healpix_pixels()) == 1

    assert is_valid_catalog(small_sky_dir_cloud)


def test_empty_directory(tmp_cloud_path):
    """Test loading empty or incomplete data"""
    catalog_path = tmp_cloud_path

    ## Path exists but there's nothing there (which means it doesn't exist!)
    with pytest.raises(FileNotFoundError, match="No directory"):
        Catalog.read_from_hipscat(catalog_path)

    ## catalog_info file exists - getting closer
    file_name = catalog_path / "catalog_info.json"
    file_io.write_string_to_file(file_name, string='{"catalog_name":"empty", "catalog_type":"source"}')

    with pytest.raises(FileNotFoundError, match="metadata"):
        Catalog.read_from_hipscat(catalog_path)

    ## partition_info file exists - enough to create a catalog
    ## Now we create the needed _metadata and everything is right.
    part_info = PartitionInfo.from_healpix([HealpixPixel(0, 11)])
    part_info.write_to_metadata_files(catalog_path=catalog_path)

    with pytest.warns(UserWarning, match="slow"):
        catalog = Catalog.read_from_hipscat(catalog_path)
    assert catalog.catalog_name == "empty"
