import os

import hipscat_import.index.run_index as runner
import pyarrow as pa
from hipscat.catalog.dataset.dataset import Dataset
from hipscat.io.file_io import read_parquet_metadata
from hipscat_import.index.arguments import IndexArguments


def test_run_index(
    small_sky_order1_dir_local,
    tmp_path,
    tmp_cloud_path,
    storage_options,
    dask_client,
):
    """Test appropriate metadata is written"""

    args = IndexArguments(
        input_catalog_path=small_sky_order1_dir_local,
        indexing_column="id",
        output_path=tmp_cloud_path,
        output_artifact_name="small_sky_object_index",
        output_storage_options=storage_options,
        tmp_dir=tmp_path,
        dask_tmp=tmp_path,
        progress_bar=False,
    )
    runner.run(args, dask_client)

    # Check that the catalog metadata file exists
    catalog = Dataset.read_from_hipscat(args.catalog_path, storage_options=storage_options)
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path

    basic_index_parquet_schema = pa.schema(
        [
            pa.field("_hipscat_index", pa.uint64()),
            pa.field("Norder", pa.uint8()),
            pa.field("Dir", pa.uint64()),
            pa.field("Npix", pa.uint64()),
            pa.field("id", pa.int64()),
        ]
    )

    outfile = args.catalog_path / "index" / "part.0.parquet"
    schema = read_parquet_metadata(outfile, storage_options=storage_options).schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)

    schema = read_parquet_metadata(
        args.catalog_path / "_metadata", storage_options=storage_options
    ).schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)

    schema = read_parquet_metadata(
        args.catalog_path / "_common_metadata", storage_options=storage_options
    ).schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)


def test_run_index_read_from_cloud(
    small_sky_order1_dir_cloud,
    tmp_path,
    storage_options,
    dask_client,
):
    """Test appropriate metadata is written"""

    args = IndexArguments(
        input_catalog_path=small_sky_order1_dir_cloud,
        input_storage_options=storage_options,
        indexing_column="id",
        output_path=tmp_path,
        output_artifact_name="small_sky_object_index",
        tmp_dir=tmp_path,
        dask_tmp=tmp_path,
        progress_bar=False,
    )
    runner.run(args, dask_client)

    # Check that the catalog metadata file exists
    catalog = Dataset.read_from_hipscat(args.catalog_path, storage_options=storage_options)
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path

    basic_index_parquet_schema = pa.schema(
        [
            pa.field("_hipscat_index", pa.uint64()),
            pa.field("Norder", pa.uint8()),
            pa.field("Dir", pa.uint64()),
            pa.field("Npix", pa.uint64()),
            pa.field("id", pa.int64()),
        ]
    )

    outfile = args.catalog_path / "index" / "part.0.parquet"
    schema = read_parquet_metadata(outfile).schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)

    schema = read_parquet_metadata(args.catalog_path / "_metadata").schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)

    schema = read_parquet_metadata(args.catalog_path / "_common_metadata").schema.to_arrow_schema()
    assert schema.equals(basic_index_parquet_schema, check_metadata=False)
