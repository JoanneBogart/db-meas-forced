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

import numpy
import psycopg2

import lib.fits
import lib.misc
import lib.meas_algos
import lib.dbtable
import lib.sourcetable
import lib.common
import lib.config
from lib.misc import PoppingOrderedDict

if lib.config.MULTICORE:
    from lib import pipe_printf

import glob
import io
import itertools
import os
import re
import sys
import textwrap


def main():
    import argparse
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Read a rerun directory to create "meas" summary table.')

    parser.add_argument('rerunDir', help="Rerun directory from which to read data")
    parser.add_argument('schemaName', help="DB schema name in which to load data")

    parser.add_argument("--table-name", default="meas", help="Top-level table's name")
    parser.add_argument("--table-space", default="", help="DB table space for tables")
    parser.add_argument("--index-space", default="", help="DB table space for indexes")
    parser.add_argument("--db-server", metavar="key=value", nargs="+", action="append", help="DB to connect to. This option must come later than non-optional arguments.")

    parser.add_argument("--with-skymap-wcs", action="store",
        help="""Use skymap_wcs at the specified path instead of calexp-*.fits.
            To generate skymap_wcs, run 'generate-skymap_wcs.py skyMap.pickle'
        """
    )

    parser.add_argument('--create-index',  action='store_true',
       help="Create index (only; don't insert data)")

    parser.add_argument('--dry-run', dest='dryrun', action='store_true',
                        help="Do not write to db. Ignored for create-index", 
                        default=False)
    parser.add_argument('--no-insert', dest='no_insert', action='store_true',
                        help="Just create tables and views; no inserts", 
                        default=False)

    args = parser.parse_args()

    if args.db_server:
        lib.config.dbServer.update(keyvalue.split('=', 1) for keyvalue in itertools.chain.from_iterable(args.db_server))

    lib.config.tableSpace = args.table_space
    lib.config.indexSpace = args.index_space
    lib.config.withSkymapWcs = args.with_skymap_wcs

    filters = lib.common.get_existing_filters(args.rerunDir, hsc=False)
    if args.create_index:
        create_index_on_mastertable(args.rerunDir, args.schemaName, filters)
    else:
        create_mastertable_if_not_exists(args.rerunDir, args.schemaName, args.table_name, filters)

        sys.stdout.flush()
        sys.stderr.flush()
        if not args.no_insert:
            insert_into_mastertable(args.rerunDir, args.schemaName, 
                                    args.table_name, filters, args.dryrun)


def create_mastertable_if_not_exists(rerunDir, schemaName, masterTableName, 
                                     filters, dryrun):
    """
    Create the master table if it does not exist.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param masterTableName
        Name of the master table
    @param filters
        List of filter names
    """
    bNeedCreating = False

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        try:
            cursor.execute('SELECT 0 FROM "{schemaName}"."{masterTableName}" WHERE FALSE;'.format(**locals()))
        except psycopg2.ProgrammingError:
            bNeedCreating = True
            db.rollback()

    create_schema_string = 'CREATE SCHEMA IF NOT EXISTS "{schemaName}"'.format(**locals())
    if (not dryrun):
        if bNeedCreating:
            with db.cursor() as cursor:
                cursor.execute(create_schema_string)
                create_mastertable(cursor, rerunDir, schemaName, 
                                   masterTableName, filters)
            db.commit()
        else:
            db.close()
            drop_index_from_mastertable(rerunDir, schemaName, filters)
    else:
        print("Would execute: ")
        print(create_schema_string)
        cursor = None
        create_mastertable(cursor, rerunDir, schemaName, masterTableName,
                           filters)


def create_mastertable(cursor, rerunDir, schemaName, masterTableName, filters):
    """
    Create the master table.
    The master table will actually be a view, which JOINs (not UNIONs) child tables.
    The child tables will also be created during a call to this function.

    This function only creates these tables. It does not insert data into them.

    @param cursor
        DB connection's cursor object.  If None, it's a dryrun
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param masterTableName
        Name of the master table
    @param filters
        List of filter names
    """
    tract, patch, filter = get_an_exisiting_catalog_id(rerunDir)
    catPath = get_catalog_path(rerunDir, tract, patch, filter)
    tablePosition, multibands = get_catalog_schema_from_file(catPath, None)

    for table in itertools.chain([tablePosition], multibands.values()):
        table.set_filters(filters)

    for table in itertools.chain([tablePosition], multibands.values()):
        table.transform(rerunDir, tract, patch, filter, tablePosition.coords[filter])

    # Create source tables
    for table in itertools.chain([tablePosition], multibands.values()):
        table.create(cursor, schemaName)

    # Create master table
    commentOnTable = textwrap.dedent("""
    The summary table of unforced measurements on coadd images.
    </p><p>Fluxes are in CGS units, and positions are in sky coordinates.
    Shapes and ellipticities are re-projected into the planes tangent
    to the celestial sphere at the objects' own positions  (the first
    axis parallels RA, the second axis DEC; the coordinates in the tangent
    planes are flipped compared to coadd images).
    </p><p>This table is a part of the whole summary table, which has been split
    into parts due to PostgreSQL's technical limit of the number of columns permitted
    in a table. To get columns in two or more parts of the whole summary table, join
    the parts together:
    </p><pre>
      SELECT ... FROM
        {schemaName}.meas
        LEFT JOIN {schemaName}.meas2 USING (object_id)
        LEFT JOIN {schemaName}.meas3 USING (object_id)
        ...
    </pre><p>Use 'LEFT JOIN' instead of 'JOIN' and place the plain 'meas'
    (the one without suffix) at the first position in order for PostgreSQL to optimize the query.
    </p><p>The following search functions are available in where-clauses:</p><dl>
      <dt><b>coneSearch</b>(coord, RA[deg], DEC[deg], RADIUS[arcsec]) -> boolean</dt>
      <dd>This function returns True if <code>coord</code> is within a circle at (RA, DEC) with its radius RADIUS.
        Use gcoord, rcoord, icoord,... etc for <code>coord</code>.</dd>
      <dt><b>boxSearch</b>(coord, RA1, RA2, DEC1, DEC2) -> boolean</dt>
      <dd>This function returns True if <code>coord</code> is within a box [RA1, RA2] x [DEC1, DEC2] (Units are degrees).
        Use gcoord, rcoord, icoord,... etc for <code>coord</code>.
        Note that <code>boxSearch(coord, 350, 370, DEC1, DEC2)</code> is different from <code>boxSearch(coord, 350, 10, DEC1, DEC2).</code>
        In the former, ra \in [350, 360] U [0, 10]; while in the latter, ra \in [10, 350].</dd>
      <dt><b>tractSearch</b>(object_id, TRACT) -> boolean</dt>
      <dd>This function returns True if tract = TRACT.</dd>
      <dt><b>tractSearch</b>(object_id, TRACT1, TRACT2) -> boolean</dt>
      <dd>This function returns True if tract \in [TRACT1, TRACT2].</dd>
    </dl><p>
    Use of these functions will significantly speed up your query.
    </p><p>
    Field search functions are also available in where-clauses. They are used like:
    <code>SELECT ... FROM {schemaName}.meas WHERE {schemaName}.search_####(object_id) AND isprimary;</code>
    (#### is a field name) To get the full list of field search functions, say help() to the database server:
    <code>SELECT * FROM help('{schemaName}.search_%');</code>
    """).strip().format(**locals())

    # Because the number of columns exceeds PostgreSQL's limit,
    # we divide the master table into "meas", "meas2", "meas3", ...

    listUniversals = [ {"meas_position": tablePosition} ]
    listMultibands = [ multibands ]
    listMultibands.append(multibands.pop_many([
        "_meas:part2"
    ]))
    listMultibands.append(multibands.pop_many([
        "_meas:part3",
    ]))
    listMultibands.append(multibands.pop_many([
        "_meas:part4",
    ]))

    for iPart, (universals, multibands) in enumerate(
        itertools.zip_longest(listUniversals, listMultibands, fillvalue=PoppingOrderedDict()),
        start=1
    ):
        dbTables = [table.name for table in itertools.chain(universals.values(), multibands.values())]
        if len(dbTables) > 1:
            dbTables = [ '"{}"."{}"'.format(schemaName, dbTables[0]) ] + [
                'LEFT JOIN "{}"."{}" USING (object_id)'.format(schemaName, table)
                for table in dbTables[1:]
            ]
            dbTables = """
            """.join(dbTables)
        else:
            dbTables = '"{}"."{}"'.format(schemaName, dbTables[0])

        fieldDefs = []
        for table in universals.values():
            fieldDefs += table.get_exported_fields("")

        for filter in filters:
            for table in multibands.values():
                fieldDefs += table.get_exported_fields(filter)

        fieldDefs.insert(0, ("object_id", "object_id", "",
            "Unique ID in 64bit integer. Be careful not to have it converted to a 32bit integer or 64bit floating point."
        ))

        sFieldDefs = """,
        """.join(
            '{} AS {}'.format(definition, name) for name, definition, unit, doc in fieldDefs
        )

        tableName = "{masterTableName}{iPart}".format(**locals()) if iPart > 1 else masterTableName

        view_string = """
        CREATE VIEW "{schemaName}"."{tableName}" AS (
            SELECT
                {sFieldDefs}
            FROM
                {dbTables}
        )
        """.format(**locals())

        if cursor is not None:
            cursor.execute(view_string)
        else:
            print(view_string)

        for name, definition, unit, doc in fieldDefs:
            field_string = """
            COMMENT ON COLUMN "{schemaName}"."{tableName}"."{name}" IS %(comment)s
            """.format(**locals())
            arg_dict = dict(comment = "{doc} || {unit}".format(**locals()))

            if cursor is not None:
                cursor.execute(field_string, arg_dict)
            else:
                print(field_string)
                print(' to be bound with dict:  ', arg_dict)

        table_comment="""
        COMMENT ON VIEW "{schemaName}"."{tableName}" IS %(comment)s
        """.format(**locals())
        arg_dict = dict(comment = commentOnTable)
        
        if cursor is not None:
            cursor.execute(table_comment, arg_dict)
        else:
            print(table_comment)
            print(' to be bound with dict: ',arg_dict)

def insert_into_mastertable(rerunDir, schemaName, masterTableName, filters,
                            dryrun):
    """
    Insert data into the master table.
    The data will actually flow not into the master table but into its children.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param masterTableName
        Name of the master table
    @param filters
        List of filter names
    @param dryrun
    """
    for tract in lib.common.get_existing_tracts(rerunDir):
        for patch in get_existing_patches(rerunDir, tract):
            insert_patch_into_mastertable(rerunDir, schemaName, masterTableName, filters, tract, patch, dryrun)


def insert_patch_into_mastertable(rerunDir, schemaName, masterTableName, filters, tract, patch, dryrun):
    """
    Insert a specific patch into the master table.
    The data will actually flow not into the master table but into its children.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param masterTableName
        Name of the master table
    @param filters
        List of filter names
    @param tract
        Tract number.
    @param patch
        Patch number (x*100 + y)
    @param dryrun
    """
    catPaths = {}
    for filter in filters:
        catPath = get_catalog_path(rerunDir, tract, patch, filter)
        if lib.common.path_exists(catPath):
            catPaths[filter] = catPath

    if not catPaths:
        return

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        if not dryrun:
            use_cursor = cursor
            if is_patch_already_inserted(cursor, schemaName, tract, patch, catPaths.keys()):
                lib.misc.warning("Skip because already inserted: (tract,patch) = ({tract}, {patch})".format(**locals()))
                return
        else:
            use_cursor = None

        tablePosition = None
        multibands = {}
        for filter, catPath in catPaths.items():
            tablePosition, mult = get_catalog_schema_from_file(catPath, tablePosition)

            for table in mult.values():
                table.transform(rerunDir, tract, patch, filter, tablePosition.coords[filter])

                if table.name not in multibands:
                    multibands[table.name] = []
                multibands[table.name].append((table, filter))

        object_id = tablePosition.object_id
        tablePosition.transform(rerunDir, tract, patch, "", None)
        insert_patch_into_universaltable(use_cursor, schemaName, tablePosition, object_id)

        for tables in multibands.values():
            insert_patch_into_multibandtable(use_cursor, schemaName, tables, object_id)

    if not dryrun:
        db.commit()


def insert_patch_into_universaltable(cursor, schemaName, table, object_id):
    """
    Insert a patch into a universal table.
    'Universal' means 'Its contents are universal to all bands.'
    @param cursor
        DB connection's cursor object
    @param schemaName
        Name of the schema in which to locate the master table
    @param table
        DBTable_BandIndependent object
    @param object_id
        numpy.array of object ID. This is used as the primary key.
    """
    return insert_patch_into_multibandtable(cursor, schemaName, [(table, "")], object_id)

def insert_patch_into_multibandtable(cursor, schemaName, tables, object_id):
    """
    Insert a patch into a multiband table.
    @param cursor
        DB connection's cursor object
    @param schemaName
        Name of the schema in which to locate the master table
    @param tables
        List of (table: DBTable, filter: str).
        Tables in this list must have identical object_id (Or, identical (tract, patch))
        with different colors.
    @param object_id
        numpy.array of object ID. This is used as the primary key.
    """
    columns = [ object_id ]
    fieldNames = [ "object_id" ]
    format = "%ld"

    for table, filter in tables:
        for name, fmt, cols in table.get_backend_field_data(filter):
            columns.extend(cols)
            fieldNames.append(name)
            format += "\t" + fmt

    format += "\n"
    format = format.encode("utf-8")

    if lib.config.MULTICORE:
        fin = pipe_printf.open(format, *columns)
        if cursor is not None:
            cursor.copy_from(fin, '"{}"."{}"'.format(schemaName, table.name), 
                             sep='\t', columns=fieldNames)
    else:
        tsv = b''.join(format % tpl for tpl in zip(*columns))
        fin = io.BytesIO(tsv)
        if cursor is not None:
            cursor.copy_from(fin, '"{}"."{}"'.format(schemaName, table.name), 
                             sep='\t', size=-1, columns=fieldNames)


def create_index_on_mastertable(rerunDir, schemaName, filters):
    """
    Create indexes on the master table.
    The indexes will actually be set on the master's children.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param filters
        List of filter names
    """
    tract, patch, filter = get_an_exisiting_catalog_id(rerunDir)
    catPath = get_catalog_path(rerunDir, tract, patch, filter)

    tablePosition, multibands = get_catalog_schema_from_file(catPath, None)

    for table in itertools.chain([tablePosition], multibands.values()):
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in itertools.chain([tablePosition], multibands.values()):
            table.create_index(cursor, schemaName)
    db.commit()


def drop_index_from_mastertable(rerunDir, schemaName, filters):
    """
    Drop indexes from the master table.
    The indexes to be dropped has actually been set on the master's children.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param schemaName
        Name of the schema in which to locate the master table
    @param filters
        List of filter names
    """
    tract, patch, filter = get_an_exisiting_catalog_id(rerunDir)
    catPath = get_catalog_path(rerunDir, tract, patch, filter)

    tablePosition, multibands = get_catalog_schema_from_file(catPath, None)

    for table in itertools.chain([tablePosition], multibands.values()):
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in itertools.chain([tablePosition], multibands.values()):
            table.drop_index(cursor, schemaName)
    db.commit()


def get_catalog_schema_from_file(path, tablePosition):
    """
    Get fields in a "meas-*.fits" file.
    @param path
        Path to a "ref-*.fits" file
    @param tablePosition
        DBTable_Position object (optional).
    @return (tablePosition, dbtables)
        * tablePosition: DBTable_Position object.
            It is the same object (though modified)
            that was passed in as one of the arguments
            if it was not None.
        * dbtables: PoppingOrderedDict mapping name: str -> table: DBTable.
    """

    table = lib.sourcetable.SourceTable.from_hdu(lib.fits.fits_open(path)[1])

    these_object_id = table.cutout_subtable("id").fields["id"].data

    object_id = tablePosition.object_id if tablePosition is not None else None
    tract, patch, filter = lib.common.path_decompose(path)

    if (object_id is not None) and (not numpy.all(these_object_id == object_id)):
        raise RuntimeError("object_id in meas doesn't agree with other of different filter: " + path)

    algos = PoppingOrderedDict(
        (name, algoclass(table))
        for name, algoclass in lib.meas_algos.meas_algos.items()
    )

    coordfields = table.fields.pop_many(["coord_ra", "coord_dec"])

    # To suppress warnings "Ignored field: ...",
    # ignore the fields explicitly.
    def ignore(prefix):
        table.cutout_subtable(prefix)

    for name in lib.meas_algos.meas_algos_ignored:
        ignore(name)

    for field in table.fields:
        lib.misc.warning('Ignored field: ', field, 'in', path)

    if tablePosition is None:
        tablePosition = DBTable_Position(
            "_meas:position",
            algos.pop_many(["meas_coord", "merge"]),
            these_object_id,
        )
    else:
        # just discard
        algos.pop_many(["meas_coord", "merge"])

    tablePosition.add_coord(filter, coordfields)


    dbtables = PoppingOrderedDict()
    def add(name, sourcenames, dbtable_class=lib.dbtable.DBTable):
        dbtables[name] = dbtable_class(name, algos.pop_many(sourcenames))

    add("_meas:part1", [
         "base_PixelFlags",
         "calib",
         "detect",
         "deblend",
         "base_Blendedness",
         "base_ClassificationExtendedness",
         "base_FootprintArea",
         "base_InputCount",
         "base_Variance",
         "base_LocalBackground",
         #"subaru_FilterFraction",
         "meas_modelfit_CModel",
    ])


    add("_meas:part2", [
         "base_NaiveCentroid",
         "base_SdssCentroid",
         "base_GaussianFlux",
         "base_PsfFlux",
         "ext_photometryKron_KronFlux",
         "base_SdssShape",
         "ext_shapeHSM",
         "modelfit_DoubleShapeletPsfApprox",
    ])

    add("_meas:part3", [
         "base_CircularApertureFlux",
    ])

    add("_meas:part4", [
         "ext_convolved_ConvolvedFlux",
    ])

    if algos:
        raise RuntimeError("Algorithms remain unused: ".format(algos))

    return tablePosition, dbtables


class DBTable_Position(lib.dbtable.DBTable_BandIndependent):
    def __init__(self, name, algos, object_id):
        lib.dbtable.DBTable_BandIndependent.__init__(self, name, algos)
        self.object_id = object_id
        self.coords = {}

    def add_coord(self, filter, fields):
        """
        Add a coordinate field.
        @param filter (str)
            Filter name
        @param fields
            { "coord_ra": numpy.array, "coord_dec": numpy.array }
            The angles are in *radians* .
        """
        coord = self.algos["meas_coord"].add_coord(filter, fields)
        self.coords[filter] = coord

    def create_index(self, cursor, schemaName):
        lib.dbtable.DBTable_BandIndependent.create_index(self, cursor, schemaName)

        indexSpace = lib.config.get_index_space()

        cursor.execute("""
        CREATE INDEX
            "{self.name}_parent_id_idx"
        ON
            "{schemaName}"."{self.name}"
            ( parent_id
            )
        {indexSpace}
        """.format(**locals())
        )
        cursor.execute("""
        CREATE INDEX
            "{self.name}_skymap_id_idx"
        ON
            "{schemaName}"."{self.name}"
            ( public.skymap_from_object_id(object_id)
            )
        {indexSpace}
        """.format(**locals())
        )
        for filter in self.filters:
            filt = lib.common.filterToShortName[filter] + "_"
            cursor.execute("""
            CREATE INDEX
                "{self.name}_{filt}coord_idx"
            ON
                "{schemaName}"."{self.name}"
            USING GiST
                ( {filt}coord
                )
            {indexSpace}
            WHERE
                {filt}coord IS NOT NULL
            """.format(**locals())
            )

    def drop_index(self, cursor, schemaName):
        lib.dbtable.DBTable_BandIndependent.drop_index(self, cursor, schemaName)

        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_parent_id_idx"
        """.format(**locals())
        )
        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_skymap_id_idx"
        """.format(**locals())
        )
        for filter in self.filters:
            filt = lib.common.filterToShortName[filter] + "_"
            cursor.execute("""
            DROP INDEX IF EXISTS
                "{schemaName}"."{self.name}_{filt}coord_idx"
            """.format(**locals())
            )


def get_existing_patches(rerunDir, tract):
    """
    Search "rerunDir" and return existing patches in the given tract.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @param tract
        Tract number.
    @return
        List of tract numbers, sorted.
    """
    patches = set(
        os.path.basename(os.path.dirname(path))
        for path in glob.iglob("{rerunDir}/deepCoadd-results/*/{tract}/*,*/meas-*".format(**locals()))
        if path.endswith('.fits') or path.endswith('.fits.gz')
    )

    return sorted(set(
        lib.common.patch_to_number(patch) for patch in patches if re.match(r'^[0-9]+,[0-9]+$', patch)
    ))


def get_catalog_path(rerunDir, tract, patch, filter):
    """
    Get the path to the "meas-*.fits" catalog identified by the arguments.
    The file may be compressed, but the path is virtualized so it always ends with '.fits'.
    """
    if patch == "*":
        x, y = "*", "*"
    else:
        x, y = patch // 100, patch % 100
    return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/meas-{filter}-{tract}-{x},{y}.fits".format(**locals())

def get_an_exisiting_catalog_id(rerunDir):
    """
    Get any one triple (tract, patch, filter) for which catalog files exist
    """
    pattern = get_catalog_path(rerunDir, "*", "*", "*")

    for imagePath in itertools.chain(glob.iglob(pattern), glob.iglob(pattern + ".gz")):
        tract, patch, filter = lib.common.path_decompose(imagePath)
        return tract, patch, filter

    raise RuntimeError("No catalog (meas-*.fits) exists.")


def is_patch_already_inserted(cursor, schemaName, tract, patch, filters):
    """
    Check whether (tract, patch, filters) has already been inserted into the DB.
    This is achieved by using a temporary table in the DB.

    This function will return
        - False if no catalogs have been inserted that match (tract, patch, *).
        - True if for every filter in filters, (tract, patch, filter) has been inserted
            and not for other filters.
        - Raise exception if there are some records that match (tract, patch, *),
            but the filter set is different from the given argument.

    @param cursor
        DB connection's cursor object
    @param schemaName
        Name of the schema in which to locate the master table
    @param tract
        Tract number
    @param patch
        Patch number (x*100 + y)
    @param filters
        List of filter names for which multiband catalogs actually exist
    """

    # file_id = (tract*10000 + patch)*100 + filter
    patchId = tract*10000 + patch
    minFileId =  patchId   *100
    maxFileId = (patchId+1)*100 - 1

    # Filter ID 0 is reserved and actual filter IDs start with 1, hence "filterOrder[f]+1"
    fileId = sorted(patchId*100 + lib.common.filterOrder[f]+1 for f in filters)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS "{schemaName}"."_temp:meas_patch" (
        file_id   Bigint   PRIMARY KEY
    )
    """.format(**locals())
    )

    if cursor is None:   return False

    cursor.execute("""
    SELECT file_id FROM "{schemaName}"."_temp:meas_patch" WHERE
        file_id BETWEEN {minFileId} AND {maxFileId}
    """.format(**locals())
    )

    dbFileId = sorted(id for id, in cursor)

    if fileId == dbFileId:
        return True

    if dbFileId:
        # This patch has already been registered in the DB
        # but the previous filter set was different from the current one.
        fileId = set(fileId)
        dbFileId = set(dbFileId)
        added = fileId - dbFileId
        subed = dbFileId - fileId

        toFilterName = dict((i+1, filter) for filter, i in lib.common.filterOrder.items())
        added = '(added:' + ','.join(toFilterName[i % 100] for i in added)+')' if added else ""
        subed = '(absent:' + ','.join(toFilterName[i % 100] for i in subed)+')' if subed else ""
        raise RuntimeError(
            ("Existing filters for (tract,patch)=({tract}, {patch}) has been changed. "
            + "{added}{subed}")
            .format(**locals())
        )

    sFileId = ",".join("({})".format(id) for id in fileId)

    cursor.execute("""
    INSERT INTO "{schemaName}"."_temp:meas_patch"
    VALUES {sFileId}
    """.format(**locals())
    )

    return False


if __name__ == "__main__":
    main()
