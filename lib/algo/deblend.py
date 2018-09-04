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


class Algo_deblend(algobase.Algo):
    positions = [
        {
            'x'  : 'deblend_psfCenter_x',
            'y'  : 'deblend_psfCenter_y',
            'ra' : 'deblend_psfCenter_ra',
            'dec': 'deblend_psfCenter_dec',
        },
    ]

    fluxes = [
        {
            "flux": 'deblend_psfFlux',
            "mag": 'deblend_psfMag',
        },
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("deblend_")
