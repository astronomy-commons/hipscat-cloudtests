"""Tests of file IO (reads and writes)"""

import os

import hipscat.io.write_metadata as io
import hipscat.pixel_math as hist
import numpy.testing as npt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hipscat.catalog.catalog_info import CatalogInfo
from hipscat.io import file_io
from hipscat.io.file_io.file_io import get_fs
from hipscat.io.parquet_metadata import write_parquet_metadata

from hipscat_cloudtests import assert_text_file_matches


@pytest.fixture
def basic_catalog_parquet_metadata():
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("ra", pa.float64()),
            pa.field("dec", pa.float64()),
            pa.field("ra_error", pa.int64()),
            pa.field("dec_error", pa.int64()),
            pa.field("Norder", pa.uint8()),
            pa.field("Dir", pa.uint64()),
            pa.field("Npix", pa.uint64()),
            pa.field("_hipscat_index", pa.uint64()),
        ]
    )


@pytest.fixture
def catalog_info_data() -> dict:
    return {
        "catalog_name": "test_name",
        "catalog_type": "object",
        "total_rows": 10,
        "epoch": "J2000",
        "ra_column": "ra",
        "dec_column": "dec",
    }


@pytest.fixture
def catalog_info(catalog_info_data) -> CatalogInfo:
    return CatalogInfo(**catalog_info_data)


def test_write_catalog_info(tmp_cloud_path, catalog_info, storage_options):
    """Test that we accurately write out catalog metadata"""
    catalog_base_dir = tmp_cloud_path
    expected_lines = [
        "{",
        '    "catalog_name": "test_name",',
        '    "catalog_type": "object",',
        '    "total_rows": 10,',
        '    "epoch": "J2000",',
        '    "ra_column": "ra",',
        '    "dec_column": "dec"',
        "}",
    ]

    io.write_catalog_info(
        dataset_info=catalog_info,
        catalog_base_dir=catalog_base_dir,
        storage_options=storage_options,
    )
    metadata_filename = os.path.join(catalog_base_dir, "catalog_info.json")
    assert_text_file_matches(expected_lines, metadata_filename, storage_options=storage_options)


def test_write_provenance_info(tmp_cloud_path, catalog_info, storage_options):
    """Test that we accurately write out tool-provided generation metadata"""
    catalog_base_dir = tmp_cloud_path
    expected_lines = [
        "{",
        '    "catalog_name": "test_name",',
        '    "catalog_type": "object",',
        '    "total_rows": 10,',
        '    "epoch": "J2000",',
        '    "ra_column": "ra",',
        '    "dec_column": "dec",',
        r'    "version": ".*",',  # version matches digits
        r'    "generation_date": "[.\d]+",',  # date matches date format
        '    "tool_args": {',
        '        "tool_name": "hipscat-import",',
        '        "tool_version": "1.0.0",',
        r'        "input_file_names": \[',
        '            "file1",',
        '            "file2",',
        '            "file3"',
        "        ]",
        "    }",
        "}",
    ]

    tool_args = {
        "tool_name": "hipscat-import",
        "tool_version": "1.0.0",
        "input_file_names": ["file1", "file2", "file3"],
    }

    io.write_provenance_info(
        catalog_base_dir=catalog_base_dir,
        dataset_info=catalog_info,
        tool_args=tool_args,
        storage_options=storage_options,
    )
    metadata_filename = os.path.join(catalog_base_dir, "provenance_info.json")
    assert_text_file_matches(expected_lines, metadata_filename, storage_options=storage_options)


def test_write_parquet_metadata(
    tmp_cloud_path,
    small_sky_dir_cloud,
    basic_catalog_parquet_metadata,
    storage_options,
):
    """Use existing catalog parquet files and create new metadata files for it"""
    catalog_base_dir = tmp_cloud_path

    write_parquet_metadata(
        catalog_path=small_sky_dir_cloud,
        storage_options=storage_options,
        output_path=catalog_base_dir,
    )

    check_parquet_schema(
        os.path.join(catalog_base_dir, "_metadata"),
        basic_catalog_parquet_metadata,
        storage_options=storage_options,
    )
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        os.path.join(catalog_base_dir, "_common_metadata"),
        basic_catalog_parquet_metadata,
        0,
        storage_options=storage_options,
    )

    ## Re-write - should still have the same properties.
    write_parquet_metadata(
        catalog_path=small_sky_dir_cloud,
        storage_options=storage_options,
        output_path=catalog_base_dir,
    )
    check_parquet_schema(
        os.path.join(catalog_base_dir, "_metadata"),
        basic_catalog_parquet_metadata,
        storage_options=storage_options,
    )
    ## _common_metadata has 0 row groups
    check_parquet_schema(
        os.path.join(catalog_base_dir, "_common_metadata"),
        basic_catalog_parquet_metadata,
        0,
        storage_options=storage_options,
    )


def check_parquet_schema(file_name, expected_schema, expected_num_row_groups=1, storage_options: dict = None):
    """Check parquet schema against expectations"""
    assert file_io.does_file_or_directory_exist(file_name, storage_options=storage_options)

    single_metadata = file_io.read_parquet_metadata(file_name, storage_options=storage_options)
    schema = single_metadata.schema.to_arrow_schema()

    assert len(schema) == len(
        expected_schema
    ), f"object list not the same size ({len(schema)} vs {len(expected_schema)})"

    npt.assert_array_equal(schema.names, expected_schema.names)

    assert schema.equals(expected_schema, check_metadata=False)

    file_system, file_pointer = get_fs(file_name, storage_options=storage_options)
    parquet_file = pq.ParquetFile(file_pointer, filesystem=file_system)
    assert parquet_file.metadata.num_row_groups == expected_num_row_groups

    for row_index in range(0, parquet_file.metadata.num_row_groups):
        row_md = parquet_file.metadata.row_group(row_index)
        for column_index in range(0, row_md.num_columns):
            column_metadata = row_md.column(column_index)
            assert column_metadata.file_path.endswith(".parquet")


def test_read_write_fits_point_map(tmp_cloud_path, storage_options):
    """Check that we write and can read a FITS file for spatial distribution."""
    initial_histogram = hist.empty_histogram(1)
    filled_pixels = [51, 29, 51, 0]
    initial_histogram[44:] = filled_pixels[:]
    io.write_fits_map(tmp_cloud_path, initial_histogram, storage_options=storage_options)

    output_file = os.path.join(tmp_cloud_path, "point_map.fits")

    output = file_io.read_fits_image(output_file, storage_options=storage_options)
    npt.assert_array_equal(output, initial_histogram)
