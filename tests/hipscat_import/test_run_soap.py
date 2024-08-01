import os

import hipscat_import.soap.run_soap as runner
import pytest
from hipscat.catalog.association_catalog.association_catalog import AssociationCatalog
from hipscat.io.file_io import read_parquet_metadata
from hipscat_import.soap.arguments import SoapArguments


@pytest.mark.dask
def test_object_to_self_write_to_cloud(
    dask_client,
    tmp_path,
    tmp_cloud_path,
    small_sky_dir_local,
    small_sky_order1_dir_local,
    storage_options,
):
    """Test creating association between object catalogs.

    using:
    - local origin catalogs
    - writing to CLOUD

    First test creates leaf files, the second test does not (exercies different write calls).
    """
    small_sky_soap_args = SoapArguments(
        object_catalog_dir=small_sky_dir_local,
        object_id_column="id",
        source_catalog_dir=small_sky_order1_dir_local,
        source_object_id_column="id",
        source_id_column="id",
        write_leaf_files=True,
        output_artifact_name="small_sky_to_order1",
        output_path=tmp_cloud_path,
        output_storage_options=storage_options,
        progress_bar=False,
        tmp_dir=tmp_path,
    )
    runner.run(small_sky_soap_args, dask_client)

    ## Check that the association data can be parsed as a valid association catalog.
    catalog = AssociationCatalog.read_from_hipscat(
        small_sky_soap_args.catalog_path, storage_options=storage_options
    )
    assert catalog.on_disk
    assert catalog.catalog_path == small_sky_soap_args.catalog_path
    assert len(catalog.get_join_pixels()) == 4
    assert catalog.catalog_info.total_rows == 131
    assert catalog.catalog_info.contains_leaf_files

    parquet_file_name = os.path.join(small_sky_soap_args.catalog_path, "Norder=0", "Dir=0", "Npix=11.parquet")
    parquet_file_metadata = read_parquet_metadata(
        parquet_file_name, file_system=file_system, storage_options=storage_options
    )
    assert parquet_file_metadata.num_row_groups == 4
    assert parquet_file_metadata.num_rows == 131
    assert parquet_file_metadata.num_columns == 8

    small_sky_soap_args = SoapArguments(
        object_catalog_dir=small_sky_dir_local,
        object_id_column="id",
        source_catalog_dir=small_sky_order1_dir_local,
        source_object_id_column="id",
        source_id_column="id",
        write_leaf_files=False,
        output_artifact_name="small_sky_to_order1_soft",
        output_path=tmp_cloud_path,
        output_storage_options=storage_options,
        progress_bar=False,
        tmp_dir=tmp_path,
    )
    runner.run(small_sky_soap_args, dask_client)

    ## Check that the association data can be parsed as a valid association catalog.
    catalog = AssociationCatalog.read_from_hipscat(
        small_sky_soap_args.catalog_path, storage_options=storage_options
    )
    assert catalog.on_disk
    assert catalog.catalog_path == small_sky_soap_args.catalog_path
    assert len(catalog.get_join_pixels()) == 4
    assert catalog.catalog_info.total_rows == 131
    assert not catalog.catalog_info.contains_leaf_files


@pytest.mark.dask
def test_object_to_self_read_from_cloud(
    dask_client,
    tmp_path,
    small_sky_dir_cloud,
    small_sky_order1_dir_cloud,
    storage_options,
):
    """Test creating association between object catalogs.

    using:
    - CLOUD origin catalogs
    - writing to local tmp

    First test creates leaf files, the second test does not (exercies different write calls).
    """
    small_sky_soap_args = SoapArguments(
        object_catalog_dir=small_sky_dir_cloud,
        object_id_column="id",
        object_storage_options=storage_options,
        source_catalog_dir=small_sky_order1_dir_cloud,
        source_object_id_column="id",
        source_id_column="id",
        source_storage_options=storage_options,
        write_leaf_files=True,
        output_artifact_name="small_sky_to_order1",
        output_path=tmp_path,
        progress_bar=False,
        tmp_dir=tmp_path,
    )
    runner.run(small_sky_soap_args, dask_client)

    ## Check that the association data can be parsed as a valid association catalog.
    catalog = AssociationCatalog.read_from_hipscat(small_sky_soap_args.catalog_path)
    assert catalog.on_disk
    assert catalog.catalog_path == small_sky_soap_args.catalog_path
    assert len(catalog.get_join_pixels()) == 4
    assert catalog.catalog_info.total_rows == 131
    assert catalog.catalog_info.contains_leaf_files

    parquet_file_name = os.path.join(small_sky_soap_args.catalog_path, "Norder=0", "Dir=0", "Npix=11.parquet")
    parquet_file_metadata = read_parquet_metadata(
        parquet_file_name, file_system=file_system, storage_options=storage_options
    )
    assert parquet_file_metadata.num_row_groups == 4
    assert parquet_file_metadata.num_rows == 131
    assert parquet_file_metadata.num_columns == 8
