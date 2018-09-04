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


class Algo_random_base_PixelFlags(algobase.Algo):
    renamerules = [
        (r'base_PixelFlags_flag', 'PixelFlags'),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("base_PixelFlags_")

        suffix = "Center"
        noncenter_flags = [
            key[:-len(suffix)] for key in self.sourceTable.fields
            if key.endswith(suffix)
        ]

        # throw away non-center flags.
        # (In hscPipe 4.x, they were named '*_any')
        self.sourceTable.fields.pop_many(noncenter_flags, default=None)
