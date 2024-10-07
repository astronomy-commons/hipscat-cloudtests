import healpy as hp
from hats.catalog import Catalog
from hats.inspection import plot_pixels, plot_points

# pylint: disable=no-member


def test_generate_map_order1(small_sky_dir_cloud, mocker):
    """Basic test that map data can be generated (does not test that a plot is rendered)"""
    cat = Catalog.read_hats(small_sky_dir_cloud)

    mocker.patch("healpy.mollview")
    plot_pixels(cat)
    hp.mollview.assert_called_once()

    hp.mollview.reset_mock()
    plot_points(cat)
    hp.mollview.assert_called_once()
