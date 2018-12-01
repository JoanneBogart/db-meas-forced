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

from . import libwcs
from . import common
from .misc import PoppingOrderedDict
from . import sourcetable

import numpy

import re


class Algo(object):
    """
    This is a base class for an "algorithm".
    An "algorithm" in this context is a measurement algorithm like
        "base_GaussianFlux", "base_SdssCentroid", ...

    This class and subclasses thereof are responsible for
        * Coordinate transformation
        * Conversion from flux to magnitude
        * Field renaming

    Subclasses must override the following class members in need.
    Fields described there will automatically converted by the base
    class (i.e. this class).

        * positions:
            list of {
                'x'  : Field name of x coordinate.   (source)
                'y'  : Field name of y coordinate.   (source)
                'ra' : Field name of ra coordinate.  (destination)
                'dec': Field name of dec coordinate. (destination)
            }

        * positionerrs:
            list of {
                'xx' : Field name of covariance xx. (source)
                'xy' : Field name of covariance xy. (source)
                'yy' : Field name of covariance yy. (source)
                'ra' : Field name of ra.            (source)
                'dec': Field name of dec.           (source)
                '11' : Field name of covariance ra-ra.   (destination)
                '12' : Field name of covariance ra-dec.  (destination)
                '22' : Field name of covariance dec-dec. (destination)
                'validity': one of ("full"|"diagonal"|"none")
            }
            The 'ra' name and 'dec' name may be ('default_ra', 'default_dec').
            Otherwise they must agree with those in positions.
            'Validity' means:
                * when "full": all members of covariance ('xx', 'xy', 'yy')
                    are valid.
                * when "diagonal": Covariance matrix is diagonal.
                    In other words, 'xy' is invalid.
                * when "none": Covariance is nonsense.

        * positionsigmas
            list of {
                'xsigma'   : Field name of sigma(x). (source)
                'ysigma'   : Field name of sigma(y). (source)
                'ra'       : Field name of ra.       (source)
                'dec'      : Field name of dec.      (source)
                'rasigma'  : Field name of sigma(ra).   (destination)
                'decsigma' : Field name of sigma(dec).  (destination)
            }
            This is a newer interface for positionerrs with validity=diagonal.

        * fluxes:
            list of {
                'flux': Field name of flux. (source)
                'mag' : Field name of magnitude. (destination, optional)
            }
            The 'mag' name is optional. If not given, it will be generated
            from the flux name.

        * fluxerrs:
            list of {
                'flux'   : Field name of flux.       (source)
                'fluxerr': Field name of flux sigma. (source)
                'magerr' : Field name of magnitude sigma. (destination, optional)
            }
            The 'magerr' name is optional. If not given, it will be generated
            from the fluxerr name.

        * sizes:
            list of {
                'size': Field name of size. (source, destination)
                'ra'  : Field name of ra.   (source)
                'dec' : Field name of dec.  (source)
            }
            Sizes in pixels will be converted to arcseconds.

        * shapes:
            list of {
                'xx' : Field name of moment xx.     (source)
                'xy' : Field name of moment xy.     (source)
                'yy' : Field name of moment yy.     (source)
                'ra' : Field name of ra.            (source)
                'dec': Field name of dec.           (source)
                '11' : Field name of moment ra-ra.   (destination)
                '12' : Field name of moment ra-dec.  (destination)
                '22' : Field name of moment dec-dec. (destination)
            }

        * shapeerrs:
            list of {
                'xx_xx' : Field name of covariance xx-xx.     (source)
                'xx_yy' : Field name of covariance xx-yy.     (source)
                'yy_yy' : Field name of covariance yy-yy.     (source)
                'xx_xy' : Field name of covariance xx-xy.     (source)
                'yy_xy' : Field name of covariance yy-xy.     (source)
                'xy_xy' : Field name of covariance xy-xy.     (source)
                'ra'    : Field name of ra.                   (source)
                'dec'   : Field name of dec.                  (source)
                '11_11' : Field name of covariance 11_11. (destination)
                '11_22' : Field name of covariance 11_22. (destination)
                '22_22' : Field name of covariance 22_22. (destination)
                '11_12' : Field name of covariance 11_12. (destination)
                '22_12' : Field name of covariance 22_12. (destination)
                '12_12' : Field name of covariance 12_12. (destination)
                'validity': one of ("full"|"diagonal"|"none")
            }
            See positionerrs.

        * shapesigmas:
            list of {
                'xxsigma': Field name of sigma(moment xx)     (source)
                'yysigma': Field name of sigma(moment xx)     (source)
                'xysigma': Field name of sigma(moment xx)     (source)
                'ra'     : Field name of ra.                  (source)
                'dec'    : Field name of dec.                 (source)
                '11sigma': Field name of sigma(moment ra-ra)   (dest.)
                '22sigma': Field name of sigma(moment dec-dec) (dest.)
                '12sigma': Field name of sigma(moment ra-dec)  (dest.)
            }
            This is a newer interface for shapeerrs with validity=diagonal.

        * ellipticities:
            list of {
                'e1' : Field name of e \\cos\\theta  (source, destination)
                'e2' : Field name of e \\sin\\theta  (source, destination)
                'ra' : Field name of ra.                  (source)
                'dec': Field name of dec.                 (source)
            }
            Ellipticity 'e' is in this context defined as
                e = (a*a - b*b) / (a*a + b*b),
            where a and b are semi-major/minor axis of the ellipse.

        * ellipticityerrs:
            Unimplemented.

        * doubleprecisions:
            list of field names.
            By default, all double-precision fields except positions
            will be truncated to single precision.
            To avoid this truncation, list field names in this list.

        * renamerules:
            list of (regex, replace).
            Field names in the source tables are too verbose.
            We should rename them with some rule, or this list.
            Each element of renamerules expresses a rename rule.
            The rules will be matched in series, and will stop
            at the first match. In each match,
                re.match(regex, field_name)
            will be performed. The matched part will be replaced
            by 'replace' (string or function). This behavior is
            similar to re.sub(regex, replace, field_name),
            but the regex will only match at the start position
            of the field_name.

            The renamerules will be applied *after* the other
            conversions. It means that field names that appear
            in "positions", "positionerrs", ... must be their
            original, long, full name.

    Subclasses must override the constructor, and set
    "self.sourceTable" member, thus:

        def __init__(self, sourceTable):
            self.sourceTable = sourceTable.cutout_subtable("base_MyFlux_")
    """

    __slots__ = ["filters"]

    positions = []
    positionerrs = []
    positionsigmas = []
    fluxes = []
    fluxerrs = []
    sizes = []
    shapes = []
    shapeerrs = []
    shapesigmas = []
    ellipticities = []
    ellipticityerrs = []
    doubleprecisions = []
    renamerules = []

    def set_filters(self, filters):
        """
        This member must be called before creating tables
        so that subclasses of this class will use the filter information
        in telling what members will exist.

        @param filters: list of filter names.
        """
        self.filters = list(filters)

    def transform(self, rerunDir, tract, patch, filter, coord):
        """
        Transform coordinates, and convert field names.
        The arguments will serve as hints in the transformation.
        @param rerunDir (str):
            Path to the rerun directory.
        @param tract (int):
            Tract number.
        @param patch (int):
            Patch number.
        @param filter (str):
            Filter name.
        @param coord (dict):
            {"ra": numpy.array (in degrees), "dec": numpy.array (in degrees)}
            Default coordinates used in transformation.
            These values will be used if subclasses set
                "ra:" "default_ra", "dec": "default_dec"
            in positionerrs, shapes, etc.
        """

        #  Almost everything is commented out. See _AlgoTransformer.transform()
        #  below.
        _AlgoTransformer(self, rerunDir, tract, patch, filter, coord).transform()
        return

    def get_backend_fields(self, prefix):
        """
        Get field names for the backend table.
        @param prefix (str)
            This prefix will be prefixed to field names.
        @return list of (fieldname, sqltype).
            The sqltype is "big integer", "double precision", etc, for example.
        """
        ret = []
        for field in self.sourceTable.fields.values():
            for f in field.explode():
                ret.append((prefix + f.name, f.get_sqltype()))

        return ret

    def get_backend_field_data(self, prefix):
        """
        Get field data for the backend table.
        @param prefix (str)
            This prefix will be prefixed to field names.
            Typical use is <filtername>_, e.g. 'g_'
        @return list of (fieldname, printf_format, [column]).
            'column' is a numpy.array. An example of the return value is:
            ("i_point", "(%.16e,%.16e)", [x, y]),
            in which x and y are numpy.array.
        """
        ret = []
        for field in self.sourceTable.fields.values():
            for f in field.explode():
                ret.append((prefix + f.name, f.get_print_format(), f.get_columns()))

        return ret

    def get_frontend_fields(self, prefix):
        """
        Get field data for the frontend view.
        @param prefix (str)
            This prefix will be prefixed to field names.
            Typical use is <filtername>_, e.g. 'g_'
        @return list of (fieldname, definition, unit, document).
            Each field can be exported as:
                {definition} AS {fieldname}.
        """
        members = []

        fluxes = {desc["flux"]: desc for desc in self.fluxes}
        fluxerrs = {desc["fluxerr"]: desc for desc in self.fluxerrs}

        for key, field in self.sourceTable.fields.items():
            if key in fluxes:
                desc = fluxes[key]
                member = prefix + field.name
                if "mag" in desc:
                    mag = prefix + self._get_renamed_name(desc["mag"])
                else:
                    mag = re.sub(r"_flux($|_)", r"_mag\1", member)
                members.append((member, get_exporting_phrase_Flux(member), "erg s^{-1} cm^{-2} Hz^{-1}", field.doc))
                members.append((mag, get_exporting_phrase_Mag(member), "mag", field.doc))
            elif key in fluxerrs:
                desc = fluxerrs[key]
                member = prefix + field.name
                flux   = prefix + self.sourceTable.fields[desc["flux"]].name
                if "magerr" in desc:
                    magerr = prefix + self._get_renamed_name(desc["magerr"])
                else:
                    magerr = re.sub(r"_fluxsigma($|_)", r"_magsigma\1", member)
                members.append((member, get_exporting_phrase_Flux_err(member), "erg s^{-1} cm^{-2} Hz^{-1}", field.doc))
                members.append((magerr, get_exporting_phrase_Mag_err(flux, member), "mag", field.doc))
            else:
                for f in field.explode():
                    member = prefix + f.name
                    members.append((member, member, f.unit, f.doc))

        return members

    @classmethod
    def _get_renamed_name(cls, nameInSourceTable):
        """
        Apply renamerules to the given name.
        No, don't we're not doing any renaing

        @param nameInSourceTable (str)
        @return (str):
            Renamed name.
        """
        return nameInSourceTable

        # class TempAlgo(Algo):
        #     renamerules = cls.renamerules
        #     def __init__(self, name):
        #         fields = PoppingOrderedDict([("", sourcetable.Field(name, None, None, None, None))])
        #         self.sourceTable = sourcetable.SourceTable(fields, None, None)

        # algoobj = TempAlgo(nameInSourceTable)
        # _AlgoTransformer(algoobj, None, None, None, None, None)._rename()
        # return algoobj.sourceTable.fields[""].name


class _AlgoTransformer(object):
    """
    This class actually performs transformations instead of Algo.
    Instance of this class will hold caches of coordinates, WCS,
    and WCS Jacobians once computed.
    """
    def __init__(self, algo, rerunDir, tract, patch, filter, coord):
        """
        @param algo (Algo):
            Instance of a subclass of Algo.
        @param rerunDir (str):
            Path to the rerun directory.
        @param tract (int):
            Tract number.
        @param patch (int):
            Patch number.
        @param filter (str):
            Filter name.
        @param coord (dict):
            {"ra": numpy.array (in degrees), "dec": numpy.array (in degrees)}
            Default coordinates used in transformation.
            These values will be used if subclasses of Algo set
                "ra:" "default_ra", "dec": "default_dec"
            in algo.positionerrs, algo.shapes, etc.
        """
        self.algoclass = type(algo)
        self.fields = algo.sourceTable.fields
        self.imagePath = (rerunDir, tract, patch, filter)

        self.wcs = None

        self.jacobians = {}
        self.posvalues = {
            "default_ra" : coord["ra"],
            "default_dec": coord["dec"],
        } if coord else {}

    def transform(self):
        """
        Do transformations
        For native DC2 data skip everything but _to_singleprecision( )
        """
        #self._transform_positions()
        #self._transform_positionerrs()
        #self._transform_positionsigmas()
        #self._transform_sizes()
        #self._transform_shapes()
        #self._transform_shapeerrs()
        #self._transform_shapesigmas()
        #self._transform_ellipticities()
        #self._transform_angles()
        self._to_singleprecision()
        #self._rename()

    def _transform_positions(self):
        for desc in self.algoclass.positions:
            ra, dec = self._get_position(desc)
            self.fields[desc["x"]] = self.fields[desc["x"]]._replace(name=desc["ra" ], data=ra , unit="degree")
            self.fields[desc["y"]] = self.fields[desc["y"]]._replace(name=desc["dec"], data=dec, unit="degree")

    def _transform_positionerrs(self):
        for desc in self.algoclass.positionerrs:
            jacobian = self._get_jacobian(desc)

            if desc["validity"] == "full":
                f11 = self.fields[desc["xx"]]
                f12 = self.fields[desc["xy"]]
                f22 = self.fields[desc["yy"]]
                e11, e12, e22 = jacobian.pixeltosky_err(f11.data, f12.data, f22.data)
                self.fields[desc["xx"]] = f11._replace(name=desc["11"], data=numpy.asarray(e11, dtype=numpy.float32), unit="arcsec^2")
                self.fields[desc["xy"]] = f12._replace(name=desc["12"], data=numpy.asarray(e12, dtype=numpy.float32), unit="arcsec^2")
                self.fields[desc["yy"]] = f22._replace(name=desc["22"], data=numpy.asarray(e22, dtype=numpy.float32), unit="arcsec^2")
            elif desc["validity"] == "diagonal":
                f11 = self.fields[desc["xx"]]
                f22 = self.fields[desc["yy"]]
                e11, e22 = jacobian.pixeltosky_err_diag(f11.data, f22.data)
                self.fields[desc["xx"]] = f11._replace(name=desc["11"], data=numpy.asarray(e11, dtype=numpy.float32), unit="arcsec^2")
                self.fields[desc["yy"]] = f22._replace(name=desc["22"], data=numpy.asarray(e22, dtype=numpy.float32), unit="arcsec^2")
                self.fields.pop(desc["xy"])
            elif desc["validity"] == "none":
                self.fields.pop(desc["xx"])
                self.fields.pop(desc["xy"])
                self.fields.pop(desc["yy"])
            else:
                raise RuntimeError("Validity must be (full|diagonal|none): {}".format(desc["validity"]))

    def _transform_positionsigmas(self):
        for desc in self.algoclass.positionsigmas:
            jacobian = self._get_jacobian(desc)

            f11 = self.fields[desc["xsigma"]]
            f22 = self.fields[desc["ysigma"]]
            e11, e22 = jacobian.pixeltosky_err_diag(numpy.square(f11.data), numpy.square(f22.data))
            self.fields[desc["xsigma"]] = f11._replace(name=desc["rasigma" ], data=numpy.sqrt(e11).astype(dtype=numpy.float32), unit="arcsec")
            self.fields[desc["ysigma"]] = f22._replace(name=desc["decsigma"], data=numpy.sqrt(e22).astype(dtype=numpy.float32), unit="arcsec")

    def _transform_sizes(self):
        for desc in self.algoclass.sizes:
            jacobian = self._get_jacobian(desc)
            pixel_scale = jacobian.pixel_scale()
            f = self.fields[desc["size"]]
            self.fields[desc["size"]] = f._replace(data=numpy.asarray(f.data*pixel_scale, dtype=numpy.float32), unit = "arcsec")

    def _transform_shapes(self):
        for desc in self.algoclass.shapes:
            jacobian = self._get_jacobian(desc)

            f11 = self.fields[desc["xx"]]
            f22 = self.fields[desc["yy"]]
            f12 = self.fields[desc["xy"]]
            e11, e22, e12 = jacobian.pixeltosky_shape(f11.data, f22.data, f12.data)
            self.fields[desc["xx"]] = f11._replace(name=desc["11"], data=numpy.asarray(e11, dtype=numpy.float32), unit="arcsec^2")
            self.fields[desc["yy"]] = f22._replace(name=desc["22"], data=numpy.asarray(e22, dtype=numpy.float32), unit="arcsec^2")
            self.fields[desc["xy"]] = f12._replace(name=desc["12"], data=numpy.asarray(e12, dtype=numpy.float32), unit="arcsec^2")

    def _transform_shapeerrs(self):
        for desc in self.algoclass.shapeerrs:
            jacobian = self._get_jacobian(desc)

            if desc["validity"] == "full":
                f11_11 = self.fields[desc["xx_xx"]]
                f11_22 = self.fields[desc["xx_yy"]]
                f22_22 = self.fields[desc["yy_yy"]]
                f11_12 = self.fields[desc["xx_xy"]]
                f22_12 = self.fields[desc["yy_xy"]]
                f12_12 = self.fields[desc["xy_xy"]]
                e11_11, e11_22, e22_22, e11_12, e22_12, e12_12 = jacobian.pixeltosky_shape_err(f11_11.data, f11_22.data, f22_22.data, f11_12.data, f22_12.data, f12_12.data)
                self.fields[desc["xx_xx"]] = f11_11._replace(name=desc["11_11"], data=numpy.asarray(e11_11, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["xx_yy"]] = f11_22._replace(name=desc["11_22"], data=numpy.asarray(e11_22, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["yy_yy"]] = f22_22._replace(name=desc["22_22"], data=numpy.asarray(e22_22, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["xx_xy"]] = f11_12._replace(name=desc["11_12"], data=numpy.asarray(e11_12, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["yy_xy"]] = f22_12._replace(name=desc["22_12"], data=numpy.asarray(e22_12, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["xy_xy"]] = f12_12._replace(name=desc["12_12"], data=numpy.asarray(e12_12, dtype=numpy.float32), unit="arcsec^4")
            elif desc["validity"] == "diagonal":
                f11_11 = self.fields[desc["xx_xx"]]
                f22_22 = self.fields[desc["yy_yy"]]
                f12_12 = self.fields[desc["xy_xy"]]
                e11_11, e22_22, e12_12 = jacobian.pixeltosky_shape_err_diag(f11_11.data, f22_22.data, f12_12.data)
                self.fields[desc["xx_xx"]] = f11_11._replace(name=desc["11_11"], data=numpy.asarray(e11_11, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["yy_yy"]] = f22_22._replace(name=desc["22_22"], data=numpy.asarray(e22_22, dtype=numpy.float32), unit="arcsec^4")
                self.fields[desc["xy_xy"]] = f12_12._replace(name=desc["12_12"], data=numpy.asarray(e12_12, dtype=numpy.float32), unit="arcsec^4")
                self.fields.pop(desc["xx_yy"])
                self.fields.pop(desc["xx_xy"])
                self.fields.pop(desc["yy_xy"])
            elif desc["validity"] == "none":
                self.fields.pop(desc["xx_xx"])
                self.fields.pop(desc["xx_yy"])
                self.fields.pop(desc["yy_yy"])
                self.fields.pop(desc["xx_xy"])
                self.fields.pop(desc["yy_xy"])
                self.fields.pop(desc["xy_xy"])

    def _transform_shapesigmas(self):
        for desc in self.algoclass.shapesigmas:
            jacobian = self._get_jacobian(desc)

            f11_11 = self.fields[desc["xxsigma"]]
            f22_22 = self.fields[desc["yysigma"]]
            f12_12 = self.fields[desc["xysigma"]]
            e11_11, e22_22, e12_12 = jacobian.pixeltosky_shape_err_diag(numpy.square(f11_11.data), numpy.square(f22_22.data), numpy.square(f12_12.data))
            self.fields[desc["xxsigma"]] = f11_11._replace(name=desc["11sigma"], data=numpy.sqrt(e11_11).astype(dtype=numpy.float32), unit="arcsec^2")
            self.fields[desc["yysigma"]] = f22_22._replace(name=desc["22sigma"], data=numpy.sqrt(e22_22).astype(dtype=numpy.float32), unit="arcsec^2")
            self.fields[desc["xysigma"]] = f12_12._replace(name=desc["12sigma"], data=numpy.sqrt(e12_12).astype(dtype=numpy.float32), unit="arcsec^2")

    def _transform_ellipticities(self):
        for desc in self.algoclass.ellipticities:
            jacobian = self._get_jacobian(desc)
            f1 = self.fields[desc["e1"]]
            f2 = self.fields[desc["e2"]]
            e1, e2 = jacobian.pixeltosky_ecc(f1.data, f2.data)
            self.fields[desc["e1"]] = f1._replace(data=e1.astype(dtype=numpy.float32), unit="arcsec^2 / arcsec^2")
            self.fields[desc["e2"]] = f2._replace(data=e2.astype(dtype=numpy.float32), unit="arcsec^2 / arcsec^2")

    def _transform_angles(self):
        for key, field in list(self.fields.items()):
            if field.type == 'Angle':
                self.fields[key] = field._replace(data=field.data*(180.0/numpy.pi), unit="degree")

    def _to_singleprecision(self):
        doubles = []
        for desc in self.algoclass.positions:
            doubles += [desc["x"], desc["y"]]

        doubles += self.algoclass.doubleprecisions

        for key, field in list(self.fields.items()):
            if field.data.dtype.name == "float64":
                if (key not in doubles) and (field.type != 'Angle'):
                    self.fields[key] = field._replace(data=field.data.astype(numpy.float32))

    def _rename(self):
        remaining_fields = list(self.fields.items())

        for regex, replacer in self.algoclass.renamerules:
            regex = re.compile(regex, re.IGNORECASE)

            unmatched_fields = []
            for key, field in remaining_fields:
                m = regex.match(field.name)
                if m:
                    suffix = field.name[m.end():]
                    newstr = replacer(m) if hasattr(replacer, "__call__") else m.expand(replacer)
                    self.fields[key] = field._replace(name=to_safe_ident(newstr + suffix))
                else:
                    unmatched_fields.append((key, field))

            remaining_fields = unmatched_fields

        for key, field in remaining_fields:
            self.fields[key] = field._replace(name=to_safe_ident(field.name))

    def _get_wcs(self):
        if self.wcs is None:
            self.wcs = libwcs.read_wcs(common.get_image_path(*self.imagePath))
        return self.wcs

    def _get_jacobian(self, desc):
        if desc["ra"] in self.jacobians:
            jacobian = self.jacobians[desc["ra"]]
        else:
            jacobian = self._get_wcs().pixeltosky_get_jacobian(*self._get_position(desc))
            self.jacobians[desc["ra"]] = jacobian

        return jacobian

    def _get_position(self, desc):
        if desc["ra"] in self.posvalues:
            ra, dec = self.posvalues[desc["ra"]], self.posvalues[desc["dec"]]
        else:
            x = self.fields[desc["x"]].data
            y = self.fields[desc["y"]].data
            ra, dec = self._get_wcs().pixeltosky(x, y)
            self.posvalues[desc["ra"]], self.posvalues[desc["dec"]] = ra, dec

        return ra, dec


def to_safe_ident(name):
    """
    Convert an identifier to a safe one: [a-z_][a-z0-9_]*
    """
    name = name.lower()
    return re.sub(r"[^A-Za-z_]", "_", name[0]) + re.sub(r"[^A-Za-z0-9_]", "_", name[1:])


def get_exporting_phrase_Flux(fluxName):
    """
    Get SQL phrase that transforms a flux stored in the child table
    to the value actually viewed by users.
    """
    return """
    public."_forced:export_flux"({fluxName})
    """.format(**locals())

def get_exporting_phrase_Flux_err(fluxErrName):
    """
    Get SQL phrase that transforms a flux err stored in the child table
    to the value actually viewed by users.
    """
    return """
    public."_forced:export_fluxerr"({fluxErrName})
    """.format(**locals())

def get_exporting_phrase_Mag(fluxName):
    """
    Get SQL phrase that transforms a magnitude stored in the child table
    to the value actually viewed by users.
    """
    return """
    public."_forced:export_mag"({fluxName})
    """.format(**locals())

def get_exporting_phrase_Mag_err(fluxName, fluxErrName):
    """
    Get SQL phrase that transforms a magnitude err stored in the child table
    to the value actually viewed by users.
    """
    return """
    public."_forced:export_magerr"({fluxName}, {fluxErrName})
    """.format(**locals())
