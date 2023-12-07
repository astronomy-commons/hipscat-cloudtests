"""Functional tests for catalog import"""

import os

import hipscat_import.catalog.run_import as runner
import pytest
from hipscat.catalog.catalog import Catalog
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader

from hipscat_cloudtests import TempCloudDirectory, assert_parquet_file_ids


@pytest.mark.dask
def test_catalog_import_write_to_cloud(
    dask_client,
    small_sky_parts_dir_local,
    tmp_dir_cloud,
    example_cloud_storage_options,
    tmp_path,
):
    """Using local CSV files, write a new catalog to the cloud bucket."""
    with TempCloudDirectory(
        tmp_dir_cloud, "write_catalog_to_cloud", example_cloud_storage_options
    ) as temp_path:
        args = ImportArguments(
            output_artifact_name="small_sky_object_catalog",
            input_path=small_sky_parts_dir_local,
            output_storage_options=example_cloud_storage_options,
            input_format="csv",
            output_path=temp_path,
            dask_tmp=tmp_path,
            highest_healpix_order=1,
            progress_bar=False,
            overwrite=True,
        )

        runner.run(args, dask_client)

        # Check that the catalog metadata file exists
        catalog = Catalog.read_from_hipscat(args.catalog_path, storage_options=example_cloud_storage_options)
        assert catalog.on_disk
        assert catalog.catalog_path == args.catalog_path
        assert catalog.catalog_info.ra_column == "ra"
        assert catalog.catalog_info.dec_column == "dec"
        assert catalog.catalog_info.total_rows == 131
        assert len(catalog.get_healpix_pixels()) == 1

        # Check that the catalog parquet file exists and contains correct object IDs
        output_file = os.path.join(args.catalog_path, "Norder=0", "Dir=0", "Npix=11.parquet")

        expected_ids = [*range(700, 831)]
        assert_parquet_file_ids(
            output_file, "id", expected_ids, storage_options=example_cloud_storage_options
        )


@pytest.mark.dask
def test_catalog_import_read_from_cloud(
    dask_client,
    small_sky_parts_dir_cloud,
    example_cloud_storage_options,
    tmp_path,
):
    """Using cloud CSV files, write a new catalog to local disk."""
    args = ImportArguments(
        output_artifact_name="small_sky_object_catalog",
        input_path=small_sky_parts_dir_cloud,
        input_storage_options=example_cloud_storage_options,
        input_format="csv",
        file_reader=CsvReader(
            storage_options=example_cloud_storage_options,
        ),
        output_path=tmp_path,
        dask_tmp=tmp_path,
        highest_healpix_order=1,
        progress_bar=False,
    )

    runner.run(args, dask_client)

    # Check that the catalog metadata file exists
    catalog = Catalog.read_from_hipscat(args.catalog_path)
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path
    assert catalog.catalog_info.ra_column == "ra"
    assert catalog.catalog_info.dec_column == "dec"
    assert catalog.catalog_info.total_rows == 131
    assert len(catalog.get_healpix_pixels()) == 1

    # Check that the catalog parquet file exists and contains correct object IDs
    output_file = os.path.join(args.catalog_path, "Norder=0", "Dir=0", "Npix=11.parquet")

    expected_ids = [*range(700, 831)]
    assert_parquet_file_ids(output_file, "id", expected_ids)


def test_read_csv_cloud(example_cloud_storage_options, small_sky_parts_dir_cloud):
    """Verify we can read the csv file into a single data frame."""
    single_file = os.path.join(small_sky_parts_dir_cloud, "catalog_00_of_05.csv")
    total_chunks = 0
    for frame in CsvReader(storage_options=example_cloud_storage_options).read(single_file):
        total_chunks += 1
        assert len(frame) == 25

    assert total_chunks == 1
