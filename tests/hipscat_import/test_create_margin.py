import hipscat_import.margin_cache.margin_cache as mc
import pytest
from hipscat.catalog.healpix_dataset.healpix_dataset import HealpixDataset
from hipscat_import.margin_cache.margin_cache_arguments import MarginCacheArguments


def test_margin_cache_gen(
    small_sky_order1_dir_local,
    tmp_path,
    tmp_cloud_path,
    dask_client,
    file_system,
    storage_options,
):
    """Test that margin cache generation works end to end.

    using:
    - local origin catalog.
    - writing to CLOUD.
    """
    with pytest.warns(UserWarning, match="smaller resolution"):
        args = MarginCacheArguments(
            margin_threshold=7200.0,
            input_catalog_path=small_sky_order1_dir_local,
            output_path=tmp_cloud_path,
            output_artifact_name="small_sky_order1_margin",
            output_storage_options=storage_options,
            dask_tmp=tmp_path,
            tmp_dir=tmp_path,
            margin_order=8,
            progress_bar=False,
        )

    assert args.catalog.catalog_info.ra_column == "ra"

    mc.generate_margin_cache(args, dask_client)

    catalog = HealpixDataset.read_from_hipscat(
        args.catalog_path, file_system=file_system, storage_options=storage_options
    )
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path


def test_margin_cache_gen_read_from_cloud(
    small_sky_order1_dir_cloud,
    tmp_path,
    storage_options,
    dask_client,
):
    """Test that margin cache generation works end to end.

    using:
    - CLOUD origin catalog
    - writing to local tmp
    """
    with pytest.warns(UserWarning, match="smaller resolution"):
        args = MarginCacheArguments(
            margin_threshold=7200.0,
            input_catalog_path=small_sky_order1_dir_cloud,
            input_storage_options=storage_options,
            output_path=tmp_path,
            output_artifact_name="small_sky_order1_margin",
            dask_tmp=tmp_path,
            tmp_dir=tmp_path,
            margin_order=8,
            progress_bar=False,
        )

    assert args.catalog.catalog_info.ra_column == "ra"

    mc.generate_margin_cache(args, dask_client)

    catalog = HealpixDataset.read_from_hipscat(args.catalog_path)
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path
