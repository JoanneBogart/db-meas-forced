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

import re
import collections

import numpy

from .misc import PoppingOrderedDict
from . import config

class SourceTable(object):
    """
    This class represents a catalog file (not a DB table).
    A subtable (table with fewer columns) are also represented by this class.

    SourceTable is responsible for:
        * Reading a catalog file.

    SourceTable can be abandoned --- It was originally designed to hold
    ("fields", "slots","fitsheader"), but only "fields" is used currently.
    """
    __slots__ = ["fields", "slots", "fitsheader"]

    def __init__(self, fields, slots, fitsheader):
        """
        @param fields (PoppingOrderedDict)
            Map from name: str -> field: Field
        @param slots (dict)
            Map from alias: str -> algorithm_name: str
        @param fitsheader
            Fits header.
        """
        self.fields = fields
        self.slots  = slots
        self.fitsheader = fitsheader

    def cutout_subtable(self, prefix):
        """
        Cut out a subtable. Fiels that are cut are removed from the self.
        @param prefix (str)
            Fields whose keys start from this prefix will be cut out.
        @return (SourceTable)
        """
        included = PoppingOrderedDict()
        excluded = PoppingOrderedDict()

        for key, field in self.fields.items():
            if key.startswith(prefix):
                included[key] = field
            else:
                excluded[key] = field

        self.fields = excluded
        return SourceTable(included, self.slots, self.fitsheader)

    def dm_schema_version(self):
        if 'AFW_TABLE_VERSION' in self.fitsheader:
            return self.fitsheader['AFW_TABLE_VERSION']
        return None

    @staticmethod
    def from_hdu(hdu):
        """
        Read Fits HDU to return an instance of SourceTable.
        """
        header = hdu.header
        data   = hdu.data

        fields = PoppingOrderedDict()

        iFlag = header.get("FLAGCOL", None)

        for i in range(1, 1+header["TFIELDS"]):
            if i != iFlag:
                name   = header.get("TTYPE{}".format(i), "")
                type   = header.get("TCCLS{}".format(i), "")
                unit   = header.get("TUNIT{}".format(i), "")
                doc    = header.get("TDOC{}" .format(i), "")
                fields[name] = Field(name, type, unit, data[name], to_safe_doc(doc))
                #if 'ConvolvedFlux_seeing' in str(name):
                #    print('Found name ', str(name))

        if iFlag is not None:
            data = data["flags"]
            nFlags = int(re.match(r'^([0-9]+)X$', header["TFORM{}".format(iFlag)]).group(1))
            for i in range(1, 1+nFlags):
                name = header.get("TFLAG{}".format(i), "")
                doc  = header.get("TFDOC{}".format(i), "")
                fields[name] = Field(name, "Scalar", "", data[:,i-1], to_safe_doc(doc))

        slots = {}

        for key, value in header.items():
            if key == "ALIAS":
                reference, referend = value.split(':')
                if reference.startswith("slot_"):
                    slots[reference[len("slot_"):]] = referend
                else:
                    fields[reference] = fields[referend]._replace(name=reference)

        return SourceTable(fields, slots, header)


class Field(collections.namedtuple("Field_",
    ["name", "type", "unit", "data", "doc"]
)):
    """
    A field in a table. This is a tuple of:
      * name: Name of this field
      * type: "Scalar", "Array", "Point", "Moments", etc
      * unit: Unit of the values
      * data: numpy.array
      * doc : Document text for this field.
    """

    __slots__ = []

    def explode(self):
        """
        If this is a field of a compound value, split it into several scalars.
        Otherwise, return [self].
        @return
            List of Field objects.
        """
        if self.type in ["Scalar", "Angle"]:
            return [self]

        if self.type == "Array":
            shape = self.data.shape
            if len(shape) > 1:
                nameFmt = self.name + "{}" * len(shape[1:])
                return [
                    Field(nameFmt.format(*index), "Scalar", self.unit, self.data[(Ellipsis,)+index], self.doc)
                    for index in itertools.product(*[range(n) for n in shape[1:]])
                ]
            else:
                nameFmt = self.name + "0"
                return [
                    Field(nameFmt, "Scalar", self.unit, self.data, self.doc)
                ]

        # other types
        nameFmt = self.name + "_{}"
        return [
            Field(nameFmt.format(member), "Scalar", self.unit, self.data[..., i], self.doc)
            for i, member in enumerate(Field.typesToMembers[self.type])
        ]

    typesToMembers = {
        'Point': ["x", "y"],
        'Moments': ["11", "22", "12"],
        'Coord': ["ra", "dec"],
        'Angle': None,
        'Scalar': None,
        'Covariance(Moments)': ["11_11", "11_22", "22_22", "11_12", "22_12", "12_12"],
        'Covariance(Point)': ["11", "12", "22"],
        'Array': None,
    }

    def get_sqltype(self):
        """
        Get the type name of this field in SQL.
        """
        return Field.dtypesToSQLType[self.data.dtype.name]

    dtypesToSQLType = {
        'bool'   : "Boolean",
        'int8'   : "Smallint",
        'uint8'  : "Smallint",
        'int16'  : "Smallint",
        'uint16' : "Integer",
        'int32'  : "Integer",
        'uint32' : "Bigint",
        'int64'  : "Bigint",
        'uint64' : "Bigint",
        'float16': "Real",
        'float32': "Real",
        'float64': "Double precision",
    }

    def get_print_format(self):
        """
        Get a format string ("%d" etc) for this field.
        """
        if len(self.data.shape) > 1:
            raise RuntimeError("data must not be multi-dimensional array to print")

        name = self.data.dtype.name
        if name == 'bool':
            return "%d"
        elif name.startswith('int'):
            return "%ld"
        elif name.startswith('uint'):
            return "%lu"
        elif name == 'float32':
            return "%.8e"
        elif name == 'float64':
            return "%.16e"

        raise RuntimeError("Type not supported")

    def get_columns(self):
        """
        Accessor for self.data.
        This function will return list of iterators each of which traverses a column.
        For example, if the data is [ [1,2], [3,4] ],
        the returned value will be [ iter([1,3]), iter([2,4]) ].
        """
        if config.MULTICORE:
            if len(self.data.shape) <= 1:
                return [ self.data ]
            else:
                return [ self.data[...,i] for i in range(self.data.shape[-1]) ]
        else:
            # converting from numpy types to Python-native types accelerates formatting speed
            name = self.data.dtype.name
            if len(self.data.shape) <= 1:
                if name.startswith('float'):
                    return [(float(x) for x in self.data), ]
                elif name == 'bool':
                    return [(int(x) for x in self.data), ]
                elif name.startswith('int') or name.startswith('uint'):
                    return [(int(x) for x in self.data), ]
                else:
                    return [self.data]
            else:
                return [ (float(x) for x in self.data[...,i]) for i in range(self.data.shape[-1]) ]


class Field_earth(Field):
    """
    Field of type "earth".
    """

    @staticmethod
    def from_radec(name, ra, dec):
        """
        Create Field_earth from (ra, dec).
        @param name (str)
        @param ra (numpy.array)
            RA in *radians* .
        @param dec (numpy.array)
            Dec in *radians* .
        @return (Field_earth)
        """
        sin_ra  = numpy.sin(ra)
        cos_ra  = numpy.cos(ra)
        sin_dec = numpy.sin(dec)
        cos_dec = numpy.cos(dec)

        xyz = numpy.empty(shape=(len(sin_ra), 3), dtype=float)
        xyz[...,0] = cos_dec * cos_ra
        xyz[...,1] = cos_dec * sin_ra
        xyz[...,2] = sin_dec

        radius = 180 * 3600 / numpy.pi
        xyz *= radius

        return Field_earth(name, "Scalar", "", xyz, "")

    def get_sqltype(self):
        return "Earth"

    def get_print_format(self):
        return "(%.16e,%.16e,%.16e)"


def to_safe_doc(doc):
    """
    Convert a document string so it will be safe in HTML
    """
    return re.sub(r"[<>&]", lambda m: _toSafeDoc_escapes[m.group(0)], doc)

_toSafeDoc_escapes = {
    "<": "&lt;", ">": "&gt;", "&": "&amp;",
}
