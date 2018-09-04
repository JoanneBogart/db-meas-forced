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

from .. import algobase
from .. import sourcetable
from .. import common
from ..misc import PoppingOrderedDict


class Algo_random_coord(algobase.Algo):
    renamerules = [
        (r'detect_isPrimary', 'isPrimary'),
    ]

    def __init__(self, sourceTable):
        ra  = sourceTable.fields.pop("coord_ra").data
        dec = sourceTable.fields.pop("coord_dec").data

        fields = PoppingOrderedDict()
        fields["coord"] = sourcetable.Field_earth.from_radec("coord", ra, dec)

        ra  *= (180.0/numpy.pi)
        dec *= (180.0/numpy.pi)

        sourceTable.fields.pop("parent") # throw this away because it's always 0

        fields.update(sourceTable.fields.pop_many([
            "detect_isPrimary",
            "adjust_density"  ,
            "detect_isPatchInner",
            "detect_isTractInner",
        ]))

        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots, sourceTable.fitsheader)
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
            ("parent_id",
                "0::Bigint",
                "",
                "unique ID of parent source; always 0 (no parent).",
            ),
            ("nchild",
                "0::Integer",
                "",
                "Number of children this object has; always 0.",
            ),
            ("isprimary",
                "isprimary",
                fields["detect_isPrimary"].unit,
                fields["detect_isPrimary"].doc,
            ),
        ]

        keys_already_used = {
            "coord",
            "detect_isPrimary",
        }

        for key, field in fields.items():
            if key not in keys_already_used:
                for f in field.explode():
                    member = prefix + f.name
                    members.append((member, member, f.unit, f.doc))

        return members
