import pytest
import lsdb
from lsdb.core.crossmatch.kdtree_match import KdTreeCrossmatch


def test_kdtree_crossmatch(small_sky_catalog_cloud, small_sky_xmatch_catalog_cloud, xmatch_correct_cloud):
    with pytest.warns(RuntimeWarning, match="Results may be inaccurate"):
        xmatched = small_sky_catalog_cloud.crossmatch(
            small_sky_xmatch_catalog_cloud, radius_arcsec=0.01 * 3600, require_right_margin=False
        ).compute()
    assert len(xmatched) == len(xmatch_correct_cloud)
    for _, correct_row in xmatch_correct_cloud.iterrows():
        assert correct_row["small_sky_id"] in xmatched["id_small_sky"].values
        xmatch_row = xmatched[xmatched["id_small_sky"] == correct_row["small_sky_id"]]
        assert xmatch_row["id_small_sky_xmatch"].values == correct_row["xmatch_id"]
        assert xmatch_row["_dist_arcsec"].values == pytest.approx(correct_row["dist_arcsec"])


def test_crossmatch_with_margin(
    small_sky_order1_dir_cloud,
    small_sky_xmatch_dir_cloud,
    small_sky_margin_dir_cloud,
    xmatch_correct_cloud,
    example_cloud_storage_options,
):
    small_sky_margin_catalog = lsdb.read_hipscat(
        small_sky_margin_dir_cloud, storage_options=example_cloud_storage_options
    )
    small_sky_order1_catalog = lsdb.read_hipscat(
        small_sky_order1_dir_cloud,
        margin_cache=small_sky_margin_catalog,
        storage_options=example_cloud_storage_options,
    )
    small_sky_xmatch_catalog = lsdb.read_hipscat(
        small_sky_xmatch_dir_cloud, storage_options=example_cloud_storage_options
    )
    xmatched = small_sky_xmatch_catalog.crossmatch(
        small_sky_order1_catalog, n_neighbors=3, radius_arcsec=2 * 3600, algo=KdTreeCrossmatch
    ).compute()
    assert len(xmatched) == len(xmatch_correct_cloud)
    for _, correct_row in xmatch_correct_cloud.iterrows():
        assert correct_row["ss_id"] in xmatched["id_small_sky"].values
        xmatch_row = xmatched[
            (xmatched["id_small_sky"] == correct_row["ss_id"])
            & (xmatched["id_small_sky_xmatch"] == correct_row["xmatch_id"])
        ]
        assert len(xmatch_row) == 1
        assert xmatch_row["_dist_arcsec"].values == pytest.approx(correct_row["dist"] * 3600)
