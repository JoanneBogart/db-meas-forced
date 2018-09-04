# Copyright (C) 2016-2018  Sogo Mineo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .. import algobase


class Algo_undeblended_ext_photometryKron_KronFlux(algobase.Algo):
    sizes = [
        {
            "size": 'undeblended_ext_photometryKron_KronFlux_radius',
            "ra": 'default_ra',
            "dec": 'default_dec',
        },
        {
            "size": 'undeblended_ext_photometryKron_KronFlux_radius_for_radius',
            "ra": 'default_ra',
            "dec": 'default_dec',
        },
        {
            "size": 'undeblended_ext_photometryKron_KronFlux_psf_radius',
            "ra": 'default_ra',
            "dec": 'default_dec',
        },
    ]

    fluxes = [
        {
            "flux": 'undeblended_ext_photometryKron_KronFlux_flux',
        },
    ]

    fluxerrs = [
        {
            "flux": 'undeblended_ext_photometryKron_KronFlux_flux',
            "fluxerr": 'undeblended_ext_photometryKron_KronFlux_fluxSigma',
        },
    ]

    renamerules = [
        (r'undeblended_ext_photometryKron', 'undeblended'),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("undeblended_ext_photometryKron_KronFlux_")
