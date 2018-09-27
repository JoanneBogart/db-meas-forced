#!/usr/bin/env python

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

import lsst.afw.image as afwImage
import lsst.afw.fits as afwFits

import psycopg2

import itertools
import math
import os
import pickle
import re
import textwrap


default_db_server = {
    'dbname': os.environ.get("USER", "postgres"),
}


# The radius of the earth (in earth_distance's terminology) is
# radian / arcsec
earth = 180.0*3600.0 / math.pi

# The following parameters are common to all fits headers.
s_tract_crpix1 = '18000'
s_tract_crpix2 = '18000'
s_tract_cd1_1  = '-4.66666666666667E-05'
s_tract_cd1_2  = '0.                   '
s_tract_cd2_1  = '0.                   '
s_tract_cd2_2  = '4.66666666666667E-05 '

tract_crpix1 = int  (s_tract_crpix1)
tract_crpix2 = int  (s_tract_crpix2)
tract_cd1_1  = float(s_tract_cd1_1 )
tract_cd1_2  = float(s_tract_cd1_2 )
tract_cd2_1  = float(s_tract_cd2_1 )
tract_cd2_2  = float(s_tract_cd2_2 )


def startup():
    args = cmdline_args().parse_args()
    db_server = dict(default_db_server)
    db_server.update(key_value.split('=', 1) for key_value in itertools.chain.from_iterable(args.db_server))
    args.db_server = db_server
    main(**vars(args))


def cmdline_args():
    import argparse
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Create "skymap" table, which will contain WCS and area information.')

    parser.add_argument("skyMap_path", metavar='skymap', help="Path to skyMap.pickle. It is at RERUN/deepCoadd/skyMap.pickle.")
    parser.add_argument(
        "--db-server",
        metavar="key=value",
        nargs="+",
        default=[],
        action="append",
        help="DB to connect to. This option must come later than non-optional arguments.",
    )

    return parser


def main(skyMap_path, db_server):
    skyMap = pickle.load(open(skyMap_path, "rb"))
    db = psycopg2.connect(**db_server)

    rows = []
    for tract in skyMap:
        nx, ny = tract.getNumPatches()
        for x in range(nx):
            for y in range(ny):
                rows.append(
                    patch_to_row(tract, (x,y))
                )

        print("tract {} / {}".format(tract.getId(), len(skyMap)-1))

    create_table(db, rows)
    db.commit()


def create_table(db, rows):
    """
    Create table "skymap".

    @param db (DB Connection)
    @param rows (list of str)
        Each row must be a return value of patch_to_row().
    """
    with db.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE
        public."_skymap:base"(
            skymap_id   integer  PRIMARY KEY,
            patch_area  cube     NOT NULL,
            wcs         coaddwcs NOT NULL
        )
        """)

        cursor.execute("""
        INSERT INTO public."_skymap:base" VALUES
        """ + ",".join(rows)
        )

        cursor.execute("""
        CREATE INDEX ON public."_skymap:base" USING GiST (patch_area);
        """)

        cursor.execute("""
        CREATE OR REPLACE VIEW
        public.skymap AS (
            SELECT
                skymap_id,
            --  skymap_id / 10000                 AS tract,
            --  skymap_id % 10000                 AS patch,
            --  ((skymap_id / 100) % 100)::text
            --      || ','
            --      || (skymap_id % 100)::text    AS patch_s,
                /* position */
                patch_area,
                wcs,
                /* FITS header for WCS */
                2                     ::integer  AS naxis,
                (wcs).naxis1                     AS naxis1,
                (wcs).naxis2                     AS naxis2,
                0                     ::integer  AS pcount,
                1                     ::integer  AS gcount,
                2000.                 ::float8   AS equinox,
                'ICRS'                ::text     AS radesys,
                (wcs).crpix1                     AS crpix1,
                (wcs).crpix2                     AS crpix2,
                '{s_tract_cd1_1}'     ::float8   AS cd1_1,
                '{s_tract_cd1_2}'     ::float8   AS cd1_2,
                '{s_tract_cd2_1}'     ::float8   AS cd2_1,
                '{s_tract_cd2_2}'     ::float8   AS cd2_2,
                (wcs).crval1                     AS crval1,
                (wcs).crval2                     AS crval2,
                'deg'                 ::text     AS cunit1,
                'deg'                 ::text     AS cunit2,
                'RA---TAN'            ::text     AS ctype1,
                'DEC--TAN'            ::text     AS ctype2,
                (wcs).crpix1 - {s_tract_crpix1}::smallint AS ltv1,
                (wcs).crpix2 - {s_tract_crpix2}::smallint AS ltv2,
                {s_tract_crpix1}::smallint - (wcs).crpix1 AS crval1a,
                {s_tract_crpix2}::smallint - (wcs).crpix2 AS crval2a,
                1                     ::integer  AS crpix1a,
                1                     ::integer  AS crpix2a,
                'LINEAR'              ::text     AS ctype1a,
                'LINEAR'              ::text     AS ctype2a,
                'PIXEL'               ::text     AS cunit1a,
                'PIXEL'               ::text     AS cunit2a
            FROM
                public."_skymap:base"
        )
        """.format(**globals())
        )

        for key, value in g_comments.items():
            if key:
                object = "COLUMN public.skymap.{key}".format(key=key)
            else:
                object = "VIEW public.skymap"

            cursor.execute("""
                COMMENT ON {object} IS %(value)s
            """.format(object=object)
            , {
                "value": textwrap.dedent(value).strip(),
            })

        cursor.execute("""
        GRANT SELECT ON TABLE
            public."_skymap:base", public.skymap
        TO
            public
        """)


def patch_to_row(tract, patch_xy):
    """
    Make one VALUE string that can be used in INSERT INTO statement.

    @param tract (Tract object): an element of SkyMap object.
    @param patch_xy (2-tuple of int): patch ID
    @return a string in the format "(value_1, value_2, ..., value_n)"
    """
    patch = tract[patch_xy]
    tract_id = tract.getId()
    skymap_id = tract_id*10000 + patch_xy[0]*100 + patch_xy[1]

    bbox = patch.getOuterBBox()
    xy0 = bbox.getBegin()
    naxes = bbox.getDimensions()

    left   = xy0[0] - 0.5
    right  = xy0[0] + naxes[0] - 0.5
    bottom = xy0[1] - 0.5
    top    = xy0[1] + naxes[1] - 0.5

    wcs = tract.getWcs()
    xs, ys, zs = [], [], []
    for x, y in itertools.product([left, right], [bottom, top]):
        #coord = wcs.pixelToSky(x, y).toIcrs()
        coord = wcs.pixelToSky(x, y)
        ra  = coord.getRa ().asRadians()
        dec = coord.getDec().asRadians()
        sin_ra  = math.sin(ra )
        cos_ra  = math.cos(ra )
        sin_dec = math.sin(dec)
        cos_dec = math.cos(dec)

        xs.append(earth * (cos_ra * cos_dec))
        ys.append(earth * (sin_ra * cos_dec))
        zs.append(earth * (sin_dec         ))

    margin = get_margin(*zip(xs, ys, zs))

    patch_area = "[(%.16e,%.16e,%.16e),(%.16e,%.16e,%.16e)]" % (
        min(xs) - margin, min(ys) - margin, min(zs) - margin,
        max(xs) + margin, max(ys) + margin, max(zs) + margin,
    )

    if relative_chebyshev_distance(
        [tract_crpix1 - 1, tract_crpix2 - 1],
        wcs.getPixelOrigin()
    ) > 1e-10:
        raise RuntimeError("CRPIX is not what's expected")

    if relative_chebyshev_distance(
        [[tract_cd1_1, tract_cd1_2], [tract_cd2_1, tract_cd2_2]],
        wcs.getCdMatrix()
    ) > 1e-10:
        raise RuntimeError("CD matrix is not what's expected")

    crcoord = wcs.getSkyOrigin()

    naxis1  = naxes[0]
    naxis2  = naxes[1]
    crpix1  = tract_crpix1 - xy0[0]
    crpix2  = tract_crpix2 - xy0[1]
    crval1  = crcoord.getLongitude().asDegrees()
    crval2  = crcoord.getLatitude ().asDegrees()

    return """
    (
        '%(skymap_id)d',
        '%(patch_area)s',
        ROW(
            '%(naxis1)d',
            '%(naxis2)d',
            '%(crpix1)d',
            '%(crpix2)d',
            '%(crval1).16e',
            '%(crval2).16e'
        )
    )
    """ % locals()


def relative_chebyshev_distance(a, b):
    """
    Relative distance of array "a" and "b".
    """
    d = max(abs(i - j) for i, j in zip(flatten_array(a), flatten_array(b)))
    if d == 0.0:
        return 0.0

    norm_a = max(abs(i) for i in flatten_array(a))
    norm_b = max(abs(i) for i in flatten_array(b))

    return float(d) / max(norm_a, norm_b)


def flatten_array(a):
    """
    Flatten array "a".
    For example, [1, [2,3], [4,[5]]] -> [1, 2, 3, 4, 5]
    """
    if (
        hasattr(a, "__iter__")          # explicitly iterable
        and not raises(lambda: iter(a)) # but not numpy 0-d array
    ) or (
        hasattr(a, "__getitem__")  # not iterable but indexable
        and raises(lambda: a[()])  # and not numpy scalar
    ):
        for i in a:
            yield from flatten_array(i)
    else:
        yield a


def raises(lambd):
    """
    Returns true if lambd() raises an exception.
    """
    try:
        lambd()
        return False
    except:
        return True


r"""
We want a "patch_area" for each patch.
The patch_area is a cube (strictly speaking, rectangular parallelepiped)
that covers the patch.

The earth (or the celestial sphere) is embedded in a 3D orthogonal coordinate
system, and the edges of the "cube" must be parallel to the coordinates.

So far is the setup of this quiz.

Now, let's begin with assuming the "patch_area" is the minimal cube that
encompasses the four vertices of the patch.  Here is a problem: the patch is
not flat but curved.

Suppose the patch has the following shape when seen from upside:
                    B
                 /     \
             /             \
           A                 C    (rectangular!)
             \             /
                 \     /
                    D
But when seen horizontally, it is curved downwards:
           A                 C
             \             /      (curved downwards!)
                ---------
Then, "the minimal cube that encompasses the four vertices (A,B,C,D)" is
the thin area depicted by ====:

           A=================C
             \             /
                ---------

The cube does not cover the curved patch.

To address the problem, we simply add a "margin" in all directions.
With an appropriate margin, the cube will be:
      -----------------------------   ^
      |                           |   | margin
      |    A                 C    |   x
      |      \             /      |   | margin
      -----------------------------   v
And the cube now covers the patch.

Another problem is that the edges AB, BC, CD, DA are not straight.
Still, they are great circles: for they are straight in a gnomonic
projection.

Imagine a bow-shape seen from a fixed viewpoint, and rotate the bow around
its chord.  When does the deviation between the bow and the chord look the
largest?  It is when the bow-shape's normal vector is head-on to the
viewpoint:

                chord
           A-------------B    ^
             \         /      | deviation
               -------  bow   v

This deviation can also be absorbed by adding a similar margin:

      -------------------------   ^
      |                       |   | margin
      |    A-------------B    |   x
      |      \         /      |   | margin
      -------------------------   v

In both two cases, the margins are equal to the "versine" part of the bow-shapes.
(versine = 1 - cosine)

Which of the two "versine" is the larger?  Since the curvature radii of the
bows are the same (= earth's radius) in both cases, the longer the chord, the
larger the versine.  Therefore, that of the first case is the larger.
In that case, the length of the chord is

    2r = max(AC, BD)

where AC and BD are distances in the orthogonal coordinates (not on the
sphere).  From "r", the margin is given by:

    margin = R (1 - cos(asin(r/R))) = R - sqrt(R^2 - r^2)
        = r^2 / (R + sqrt(R^2 - r^2))

where R is the radius of the earth.
"""

def get_margin(vertex1, vertex2, vertex3, vertex4):
    r = max(
        0.5 * math.hypot(math.hypot(v1[0] - v2[0], v1[1] - v2[1]), v1[2] - v2[2])
        for v1, v2 in itertools.combinations([vertex1, vertex2, vertex3, vertex4], 2)
    )

    rIR = r / earth
    margin = r * rIR / (1.0 + math.sqrt(1 - rIR*rIR))

    # add a few tolerance
    return margin * (1.0 + 1e-4)


g_comments = {
    "": """
        In this table is stored (tract, patch) information.
        """,
    "skymap_id": """
        A number representing (tract, patch): skymap_id = tract*10000 + patch_x*100 + patch_y.
        It is, for example, 98760304 for (tract, patch) = (9876, '3,4').
        """,
    "patch_area": """
        A rectangular cuboid (called 'cube' in PostgreSQL) in the 3D space
        the sky sphere is embedded in.
        This rectangular cuboid contains the patch of the sky sphere.
        It can be used in search. See the document of PostgreSQL modules
        'cube' and 'earthdistance'.
        """,
    "wcs": """
        WCS object that can be passed to functions
        sky_to_pixel() and patch_contains().
        """,
    "naxis": """
        FITS header 'NAXIS' (Number of axes, always 2)
        """,
    "naxis1": """
        FITS header 'NAXIS1' (Width in pixels)
        """,
    "naxis2": """
        FITS header 'NAXIS2' (Height in pixels)
        """,
    "pcount": """
        FITS header 'PCOUNT'
        """,
    "gcount": """
        FITS header 'GCOUNT'
        """,
    "equinox": """
        FITS header 'EQUINOX'
        """,
    "radesys": """
        FITS header 'RADESYS'
        """,
    "crpix1": """
        FITS header 'CRPIX1'
        """,
    "crpix2": """
        FITS header 'CRPIX2'
        """,
    "cd1_1": """
        FITS header 'CD1_1'
        """,
    "cd1_2": """
        FITS header 'CD1_2'
        """,
    "cd2_1": """
        FITS header 'CD2_1'
        """,
    "cd2_2": """
        FITS header 'CD2_2'
        """,
    "crval1": """
        FITS header 'CRVAL1'
        """,
    "crval2": """
        FITS header 'CRVAL2'
        """,
    "cunit1": """
        FITS header 'CUNIT1'
        """,
    "cunit2": """
        FITS header 'CUNIT2'
        """,
    "ctype1": """
        FITS header 'CTYPE1'
        """,
    "ctype2": """
        FITS header 'CTYPE2'
        """,
    "ltv1": """
        FITS header 'LTV1'
        """,
    "ltv2": """
        FITS header 'LTV2'
        """,
    "crval1a": """
        FITS header 'CRVAL1A':
        The coordinate system 'A' is the pixel coordinates whose origin is
        the bottom-left corner of the tract (not patch).
        The center of the bottom-left pixel has coordinates (0,0) (not (1,1)).
        """,
    "crval2a": """
        FITS header 'CRVAL2A'. See the comment of crval1a.
        """,
    "crpix1a": """
        FITS header 'CRPIX1A'. See the comment of crval1a.
        """,
    "crpix2a": """
        FITS header 'CRPIX2A'. See the comment of crval1a.
        """,
    "ctype1a": """
        FITS header 'CTYPE1A'. See the comment of crval1a.
        """,
    "ctype2a": """
        FITS header 'CTYPE2A'. See the comment of crval1a.
        """,
    "cunit1a": """
        FITS header 'CUNIT1A'. See the comment of crval1a.
        """,
    "cunit2a": """
        FITS header 'CUNIT2A'. See the comment of crval1a.
        """,
}

if __name__ == "__main__":
    startup()
