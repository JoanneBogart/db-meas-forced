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


class Algo_meas_coord(algobase.Algo):
    renamerules = [
        (r'deblend_', ''),
        (r'detect_', ''),
        (r'parent', 'parent_id'),
    ]

    def __init__(self, sourceTable):
        fields = sourceTable.fields.pop_many([
            "parent"          ,
            "deblend_nChild"  ,
        ])

        self.sourceTable = sourcetable.SourceTable(fields, sourceTable.slots, sourceTable.fitsheader)

    def add_coord(self, filter, fields):
        filt = common.filterToShortName[filter] + "_"

        ra  = fields["coord_ra" ].data
        dec = fields["coord_dec"].data
        self.sourceTable.fields[filt+"coord"] = sourcetable.Field_earth.from_radec(filt+"coord", ra, dec)

        return {
            "ra" : ra *(180.0/numpy.pi),
            "dec": dec*(180.0/numpy.pi),
        }

    def get_backend_fields(self, prefix):
        # When they 'insert into table', they haven't called self.set_filters(filters).
        if self.filters is None:
            return algobase.Algo.get_backend_fields(self, prefix)

        # When they 'create table', they have called self.set_filters(filters)
        # In that case, we have to list all coord fields
        ret = []

        for filter in self.filters:
            filt = common.filterToShortName[filter] + "_"
            ret.append((filt+"coord" , "Earth"))

        for field in self.sourceTable.fields.values():
            for f in field.explode():
                if not f.name.endswith("_coord"):
                    ret.append((prefix + f.name, f.get_sqltype()))

        return ret

    def get_frontend_fields(self, prefix):
        fields = self.sourceTable.fields

        members = []
        for filter in self.filters:
            filt = common.filterToShortName[filter] + "_"
            members += [
                # exported name
                (filt+"ra",
                    # definition expression
                    "public.coord_to_ra({filt}coord)".format(**locals()),
                    # unit name
                    "degree",
                    # document
                    "RA (J2000.0) of the object in {filter}".format(**locals()),
                ),
                (filt+"dec",
                    "public.coord_to_dec({filt}coord)".format(**locals()),
                    "degree",
                    "DEC (J2000.0) of the object in {filter}".format(**locals()),
                ),
                (filt+"coord",
                    filt+"coord",
                    "",
                    "Internal value on behalf of ({filt}ra,{filt}dec). Used in coneSearch({filt}coord, RA, DEC, RADIUS) etc.".format(filt=filt),
                ),
            ]

        members += [
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
                "parent_id",
                fields["parent"].unit,
                fields["parent"].doc,
            ),
            ("nchild",
                "nchild",
                fields["deblend_nChild"].unit,
                fields["deblend_nChild"].doc,
            ),
        ]

        return members
