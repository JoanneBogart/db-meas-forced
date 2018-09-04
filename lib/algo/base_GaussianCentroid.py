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


class Algo_base_GaussianCentroid(algobase.Algo):
    positions = [
        {
            'x'  : 'base_GaussianCentroid_x',
            'y'  : 'base_GaussianCentroid_y',
            'ra' : 'base_GaussianCentroid_ra',
            'dec': 'base_GaussianCentroid_dec',
        },
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("base_GaussianCentroid_")
