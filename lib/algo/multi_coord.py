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

import numpy

#from extinction.dustval import Extinction

from .. import algobase
from .. import sourcetable
from .. import common
from ..misc import PoppingOrderedDict


class Algo_multi_coord(algobase.Algo):
    renamerules = [
        #(r'deblend_', ''),
        #(r'detect_', ''),
        #(r'parent', 'parent_id'),
    ]

    def __init__(self, sourceTable):
        #ra  = sourceTable.fields.pop("coord_ra").data
        #dec = sourceTable.fields.pop("coord_dec").data

        fields = PoppingOrderedDict()
        #fields["coord"] = sourcetable.Field_earth.from_radec("coord", ra, dec)

        fields.update(sourceTable.fields.pop_many([
            "parent"          ,
            "deblend_nChild"  ,
            "coord_ra",
            "coord_dec"
        ]))

        #print('In multi_coord __init__ fields keys are: ')
        #for k in fields: print(k)

        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots, sourceTable.fitsheader)

        #ra  *= (180.0/numpy.pi)
        #dec *= (180.0/numpy.pi)

        #self.coord = {
        #    "ra": ra, "dec": dec,
        #}

