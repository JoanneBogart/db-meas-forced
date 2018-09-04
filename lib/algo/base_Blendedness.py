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

import itertools

from .. import algobase


class Algo_base_Blendedness(algobase.Algo):
    fluxes = [
        {
            "flux": 'base_Blendedness_{}_flux_{}'.format(infix, suffix),
        }
        for infix, suffix in itertools.product(
            ["raw", "abs"],
            ["child", "parent"],
        )
    ]

    shapes = [
        {
            "xx": 'base_Blendedness_{}_{}_xx'.format(infix, suffix),
            "yy": 'base_Blendedness_{}_{}_yy'.format(infix, suffix),
            "xy": 'base_Blendedness_{}_{}_xy'.format(infix, suffix),
            "11": 'base_Blendedness_{}_{}_shape11'.format(infix, suffix),
            "22": 'base_Blendedness_{}_{}_shape22'.format(infix, suffix),
            "12": 'base_Blendedness_{}_{}_shape12'.format(infix, suffix),
            'ra' : 'default_ra',
            'dec': 'default_dec',
        }
        for infix, suffix in itertools.product(
            ["raw", "abs"],
            ["child", "parent"],
        )
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("base_Blendedness_")
