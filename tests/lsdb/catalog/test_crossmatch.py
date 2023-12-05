import pytest


def test_kdtree_crossmatch(small_sky_catalog_cloud, small_sky_xmatch_catalog_cloud, xmatch_correct_cloud):
    xmatched = small_sky_catalog_cloud.crossmatch(small_sky_xmatch_catalog_cloud).compute()
    assert len(xmatched) == len(xmatch_correct_cloud)
    for _, correct_row in xmatch_correct_cloud.iterrows():
        assert correct_row["ss_id"] in xmatched["id_small_sky"].values
        xmatch_row = xmatched[xmatched["id_small_sky"] == correct_row["ss_id"]]
        assert xmatch_row["id_small_sky_xmatch"].values == correct_row["xmatch_id"]
        assert xmatch_row["_DIST"].values == pytest.approx(correct_row["dist"])
