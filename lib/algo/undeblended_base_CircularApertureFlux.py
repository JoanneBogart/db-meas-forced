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
from .. import common

def _rename_aperture_size(m):
    prefix1, prefix2, radius, radius_frac, suffix = m.groups()
    radius = float(radius + "." + radius_frac)

    diameter = common.defaultPixelScale * 2 * radius
    diameter = str(int(round(diameter*10)))

    return prefix1 + prefix2 + diameter + suffix


class Algo_undeblended_base_CircularApertureFlux(algobase.Algo):
    fluxes = [
        {
            "flux": 'undeblended_base_CircularApertureFlux_{}_flux'.format(size),
        }
        for size in ["3_0", "4_5", "6_0", "9_0", "12_0", "17_0", "25_0", "35_0", "50_0", "70_0"]
    ]
    fluxerrs = [
        {
            "flux": 'undeblended_base_CircularApertureFlux_{}_flux'.format(size),
            "fluxerr": 'undeblended_base_CircularApertureFlux_{}_fluxSigma'.format(size),
        }
        for size in ["3_0", "4_5", "6_0", "9_0", "12_0", "17_0", "25_0", "35_0", "50_0", "70_0"]
    ]

    renamerules = [
        (r'(undeblended_)base_Circular(ApertureFlux_)([0-9]+)_([0-9]+)(_)', _rename_aperture_size),
        (r'undeblended_base_Circular', 'undeblended_'),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("undeblended_base_CircularApertureFlux_")
