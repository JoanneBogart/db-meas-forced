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


class Algo_base_SdssCentroid(algobase.Algo):
    positions = [
        {
            'x'  : 'base_SdssCentroid_x',
            'y'  : 'base_SdssCentroid_y',
            'ra' : 'base_SdssCentroid_ra',
            'dec': 'base_SdssCentroid_dec',
        },
    ]

    positionsigmas = [
        {
            'xsigma': 'base_SdssCentroid_xSigma',
            'ysigma': 'base_SdssCentroid_ySigma',
            'rasigma': 'base_SdssCentroid_raSigma',
            'decsigma': 'base_SdssCentroid_decSigma',
            'ra'  : 'base_SdssCentroid_ra',
            'dec' : 'base_SdssCentroid_dec',
        },
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("base_SdssCentroid_")
