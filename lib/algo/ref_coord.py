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

from extinction.dustval import Extinction

from .. import algobase
from .. import sourcetable
from .. import common
from ..misc import PoppingOrderedDict


class Algo_ref_coord(algobase.Algo):
    renamerules = [
        (r'deblend_', ''),
        (r'detect_', ''),
        (r'parent', 'parent_id'),
    ]

    def __init__(self, sourceTable):
        ra  = sourceTable.fields.pop("coord_ra").data
        dec = sourceTable.fields.pop("coord_dec").data

        fields = PoppingOrderedDict()
        fields["coord"] = sourcetable.Field_earth.from_radec("coord", ra, dec)

        extinction_bv = get_extinction(ra, dec)

        fields.update(sourceTable.fields.pop_many([
            "parent"          ,
            "deblend_nChild"  ,
            "detect_isPrimary",
        ]))

        fields["extinction_bv"] = sourcetable.Field(
            "extinction_bv", "Scalar", "", extinction_bv, "E(B-V)"
        )
        #print('In ref_coord __init__ fields keys are: ')
        #for k in fields: print(k)

        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots, sourceTable.fitsheader)

        ra  *= (180.0/numpy.pi)
        dec *= (180.0/numpy.pi)

        self.coord = {
            "ra": ra, "dec": dec,
        }

    def get_frontend_fields(self, prefix):
        fields = self.sourceTable.fields

        members = [
            ("ra",                              # exported name
                "public.coord_to_ra(coord)",    # definition expression
                "degree",                       # unit name
                "RA (J2000.0) of the object",   # document
            ),
            ("dec",
                "public.coord_to_dec(coord)",
                "degree",
                "DEC (J2000.0) of the object",
            ),
            ("coord",
                "coord",
                "",
                "Internal value on behalf of (ra,dec). Used in coneSearch(coord, RA, DEC, RADIUS) etc.",
            ),
            ("skymap_id",
                "public.skymap_from_object_id(object_id)",
                "",
                "Internal value on behalf of (tract, patch).",
            ),
            ("tract",
                "public.tract_from_object_id(object_id)",
                "",
                "Tract ID. *Do not* use it in where-clauses. Use tractSearch(object_id, TRACT1, TRACT2) instead.",
            ),
            ("patch",
                "public.patch_from_object_id(object_id)",
                "",
                "Patch name in an integer. It is 305 for patch (3,5), for example.",
            ),
            ("patch_s",
                "public.patch_s_from_object_id(object_id)",
                "",
                "Patch name in a string. It is '3,5' for patch (3,5), for example.",
            ),
            ("parent",
                "parent",
                fields["parent"].unit,
                fields["parent"].doc,
            ),
            ("deblend_nchild",
                "deblend_nchild",
                fields["deblend_nChild"].unit,
                fields["deblend_nChild"].doc,
            ),
            ("detect_isprimary",
                "detect_isprimary",
                fields["detect_isPrimary"].unit,
                fields["detect_isPrimary"].doc,
            ),
        ]

        #for filter in self.filters:
        #    filt = common.filterToShortName[filter]
        #    members.append(
        #        ("a_" + filt,
        #            "{}::Real * extinction_bv".format(common.absorptionCoeff[filter]),
        #            "mag",
        #            "absorption for {}".format(filter),
        #        ),
        #    )

        return members


_extinction = None

def get_extinction(ra, dec):
    """
    Get extinction E(B-V)
    @param ra
    @param dec
        numpy.array of coordinates in *radians*
    @return
        numpy.array
    """

    global _extinction
    if _extinction is None:
        _extinction = Extinction()

    return _extinction.get_Ebv(ra, dec)
