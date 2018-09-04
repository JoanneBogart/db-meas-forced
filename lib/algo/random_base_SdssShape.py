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


class Algo_random_base_SdssShape(algobase.Algo):

    shapes = [
        {
            "xx": 'base_SdssShape_psf_xx',
            "yy": 'base_SdssShape_psf_yy',
            "xy": 'base_SdssShape_psf_xy',
            "11": 'base_SdssShape_psf_shape11',
            "22": 'base_SdssShape_psf_shape22',
            "12": 'base_SdssShape_psf_shape12',
            "ra" : "default_ra",
            "dec": "default_dec",
        }
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        sdssshape = sourceTable.cutout_subtable("base_SdssShape_")
        # throw away non-psf fields
        self.sourceTable = sdssshape.cutout_subtable("base_SdssShape_psf_")
