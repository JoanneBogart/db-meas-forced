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
import sys

import lib.fits
import lib.misc
import lib.forced_algos
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
import textwrap



def main():
    import argparse
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Read a rerun directory to create "forced" summary table.')

    parser.add_argument('rerunDir', help="Rerun directory from which to read data")
    parser.add_argument('schemaName', help="DB schema name in which to load data")

    parser.add_argument("--table-name", default="forced", help="Top-level table's name")
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
    parser.add_argument('--tracts', dest='tracts', type=int, nargs='+', help="If supplied, ingest data for specified tracts only. Otherwise ingest all")
    args = parser.parse_args()

    if args.tracts is not None:
        print("Processing the following tracts:")
        for t in args.tracts: print(t)

    #   temp for debugging
    #   return

    if args.db_server:
        lib.config.dbServer.update(keyvalue.split('=', 1) for keyvalue in itertools.chain.from_iterable(args.db_server))

    lib.config.tableSpace = args.table_space
    lib.config.indexSpace = args.index_space
    lib.config.withSkymapWcs = args.with_skymap_wcs

    filters = lib.common.get_existing_filters(args.rerunDir, hsc=False)
    if args.create_index:
        create_index_on_mastertable(args.rerunDir, args.schemaName, filters)
    else:
        print("Invoking create_mastertable_if_not_exists")
        create_mastertable_if_not_exists(args.rerunDir, args.schemaName, 
                                         args.table_name, filters, args.dryrun)
        sys.stdout.flush()
        sys.stderr.flush()
        if args.tracts: 
            tracts = args.tracts
        else:
            tracts = None
        if not args.no_insert:
            print("invoking insert_into_mastertable")
            insert_into_mastertable(args.rerunDir, args.schemaName, 
                                    args.table_name, filters, args.dryrun,
                                    tracts)


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
                create_mastertable(cursor, rerunDir, schemaName, masterTableName, filters)
            db.commit()
        else:
            db.close()
            drop_index_from_mastertable(rerunDir, schemaName, filters)
    else:
        if bNeedCreating:
            print("Would execute: ")
            print(create_schema_string)
            cursor = None
            create_mastertable(cursor, rerunDir, schemaName, masterTableName, 
                               filters)
        else:
            print("Master table already exists")
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

    tract, patch, filter = get_an_existing_catalog_id(rerunDir, schemaName)
    catPath = get_catalog_path(rerunDir, tract, patch, filter, hsc=False,
                               schemaName=schemaName)
    refPath = get_ref_path   (rerunDir, tract, patch)

    universals, object_id, coord = get_ref_schema_from_file(refPath)
    multibands = get_catalog_schema_from_file(catPath, object_id)

    for table in itertools.chain(universals.values(), multibands.values()):
        table.set_filters(filters)

    for table in itertools.chain(universals.values(), multibands.values()):
        if ('position' in table.name):
            print("About to transform table ", table.name)
        table.transform(rerunDir, tract, patch, filter, coord)

    # Create source tables
    for table in itertools.chain(universals.values(), multibands.values()):
        table.create(cursor, schemaName)

    # Create master table
    commentOnTable = textwrap.dedent("""
    The summary table of forced photometry on coadd images.
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
        {schemaName}.forced
        LEFT JOIN {schemaName}.forced2 USING (object_id)
        LEFT JOIN {schemaName}.forced3 USING (object_id)
        ...
    </pre><p>Use 'LEFT JOIN' instead of 'JOIN' and place the plain 'forced'
    (the one without suffix) at the first position in order for PostgreSQL to optimize the query.
    </p><p>The following search functions are available in where-clauses:</p><dl>
      <dt><b>coneSearch</b>(coord, RA[deg], DEC[deg], RADIUS[arcsec]) -> boolean</dt>
      <dd>This function returns True if <code>coord</code> is within a circle at (RA, DEC) with its radius RADIUS.</dd>
      <dt><b>boxSearch</b>(coord, RA1, RA2, DEC1, DEC2) -> boolean</dt>
      <dd>This function returns True if <code>coord</code> is within a box [RA1, RA2] x [DEC1, DEC2] (Units are degrees).
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
    <code>SELECT ... FROM {schemaName}.forced WHERE {schemaName}.search_####(object_id) AND isprimary;</code>
    (#### is a field name) To get the full list of field search functions, say help() to the database server:
    <code>SELECT * FROM help('{schemaName}.search_%');</code>
    """).strip().format(**locals())

    # Because the number of columns exceeds PostgreSQL's limit,
    # we divide the master table into "forced", "forced2", "forced3", ...

    listUniversals = [universals]
    listMultibands = [multibands]
    listMultibands.append(multibands.pop_many([
        '_forced:part2',
    ]))
    listMultibands.append(multibands.pop_many([
        '_forced:part3',
    ]))
    (major, minor, sim_type) = extract_schema_fields(schemaName)
    if major == None or (str(major) == '1' and str(minor) == '1'):
        pass
    else:
        listMultibands.append(multibands.pop_many([
            '_forced:part4',
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
            
            fstype = type(field_string)
            if fstype is not type('a'):
                print("bad field string type ", fstype)
                print('field_string is: ')
                print(field_string)
            if cursor is not None:
                cursor.execute(field_string, arg_dict)
            else:
                print(field_string)
                print(' to be bound with dict: ',arg_dict)

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
                            dryrun, tracts):
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
        If True just print commands rather than executing
    @param tracts
        If present (not None) insert data only from specified tracts. Else
        insert data from all tracts
    """
    all_tracts = lib.common.get_existing_tracts(rerunDir)
    our_tracts = []
    if tracts == None:
        our_tracts = all_tracts
    else:
        for t in tracts:
            if t in all_tracts: our_tracts.append(t)

    for tract in our_tracts:
        for patch in get_existing_patches(rerunDir, tract):
            insert_patch_into_mastertable(rerunDir, schemaName, masterTableName, filters, tract, patch, dryrun)

            #  temp for debugging:  stop after first patch
            # if dryrun:  return

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
        If True just print commands rather than executing
    """
    catPaths = {}
    for filter in filters:
        catPath = get_catalog_path(rerunDir, tract, patch, filter, hsc=False,
                                   schemaName=schemaName)
        if lib.common.path_exists(catPath):
            catPaths[filter] = catPath

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        if not dryrun:
            use_cursor = cursor
            if is_patch_already_inserted(cursor, schemaName, tract, patch, catPaths.keys()):
                lib.misc.warning("Skip because already inserted: (tract,patch) = ({tract}, {patch})".format(**locals()))
                return
        else:
            use_cursor = None

        refPath = get_ref_path(rerunDir, tract, patch)
        universals, object_id, coord = get_ref_schema_from_file(refPath)

        for table in itertools.chain(universals.values()):
            table.transform(rerunDir, tract, patch, "", coord)

        multibands = {}
        for filter, catPath in catPaths.items():
            for table in get_catalog_schema_from_file(catPath, object_id).values():
                table.transform(rerunDir, tract, patch, filter, coord)

                if table.name not in multibands:
                    multibands[table.name] = []
                multibands[table.name].append((table, filter))

        for table in universals.values():
            insert_patch_into_universaltable(use_cursor, schemaName, table, 
                                             object_id)
        for tables in multibands.values():
            insert_patch_into_multibandtable(use_cursor, schemaName, tables, 
                                             object_id)

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
        DB connection's cursor object. If None just pretend.
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
    tract, patch, filter = get_an_existing_catalog_id(rerunDir, schemaName)
    catPath = get_catalog_path(rerunDir, tract, patch, filter, hsc=False,
                               schemaName=schemaName)
    refPath = get_ref_path    (rerunDir, tract, patch)

    universals, object_id, coord = get_ref_schema_from_file(refPath)
    multibands = get_catalog_schema_from_file(catPath, object_id)

    for table in itertools.chain(universals.values(), multibands.values()):
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in itertools.chain(universals.values(), multibands.values()):
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
    tract, patch, filter = get_an_existing_catalog_id(rerunDir, schemaName)
    catPath = get_catalog_path(rerunDir, tract, patch, filter, hsc=False,
                               schemaName=schemaName)
    refPath = get_ref_path   (rerunDir, tract, patch)

    universals, object_id, coord = get_ref_schema_from_file(refPath)
    multibands = get_catalog_schema_from_file(catPath, object_id)

    for table in itertools.chain(universals.values(), multibands.values()):
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in itertools.chain(universals.values(), multibands.values()):
            table.drop_index(cursor, schemaName)
    db.commit()


def get_ref_schema_from_file(path):
    """
    Get fields in a "ref-*.fits" file.
    @param path
        Path to a "ref-*.fits" file
    @return (dbtables, object_id, coord)
        * "dbtables" is PoppingOrderedDict mapping name: str -> table: DBTable,
        * "object_id" is a numpy.array of object_id,
        * "coord" is {"ra": numpy.array, "dec": numpy.array},
            in which angles are in degrees.
    """
    table = lib.sourcetable.SourceTable.from_hdu(lib.fits.fits_open(path)[1])
    #with  open('original_ref_fields.txt', 'w') as f:
    #    for fld in table.fields:
    #        print(fld, file=f)

    object_id = table.cutout_subtable("id").fields["id"].data

    algos = PoppingOrderedDict(
        (name, algoclass(table))
        for name, algoclass in lib.forced_algos.ref_algos.items()
    )
    #for name, algoclass in lib.forced_algos.ref_algos.items():
    #    if 'ConvolvedFlux_seeing' in name:
    #        print('name is ', name, ' and algoclass is ', str(algoclass))

    coord = algos["ref_coord"].coord

    # To suppress warnings "Ignored field: ...",
    # ignore the fields explicitly.
    def ignore(prefix):
        table.cutout_subtable(prefix)

    for name in lib.forced_algos.ref_algos_ignored:
        ignore(name)

    for field in table.fields:
        lib.misc.warning('Ignored field: ', field, 'in', path)

    dbtables = PoppingOrderedDict()
    def add(name, sourcenames, dbtable_class=lib.dbtable.DBTable_BandIndependent):
        dbtables[name] = dbtable_class(name, algos.pop_many(sourcenames))

    add("_forced:position", [
        "ref_coord", "detect", "merge",
    ], dbtable_class=DBTable_Position)

    if algos:
        print("remaining algos:")
        print(type(algos))
        for k in algos: print(str(k))
        raise RuntimeError("Algorithms remain unused; see above ")

    return dbtables, object_id, coord


class DBTable_Position(lib.dbtable.DBTable_BandIndependent):
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
        cursor.execute("""
        CREATE INDEX
            "{self.name}_coord_idx"
        ON
            "{schemaName}"."{self.name}"
        USING GiST
            ( coord
            )
        {indexSpace}
        WHERE
            coord IS NOT NULL
        """.format(**locals())
        )

        # indices WHERE isprimary = True

        cursor.execute("""
        CREATE UNIQUE INDEX
            "{self.name}_object_id_primary_idx"
        ON
            "{schemaName}"."{self.name}"
            ( object_id
            )
        {indexSpace}
        WHERE
          isprimary
        """.format(**locals())
        )
        cursor.execute("""
        CREATE INDEX
            "{self.name}_skymap_id_primary_idx"
        ON
            "{schemaName}"."{self.name}"
            ( public.skymap_from_object_id(object_id)
            )
        {indexSpace}
        WHERE
          isprimary
        """.format(**locals())
        )
        cursor.execute("""
        CREATE INDEX
            "{self.name}_coord_primary_idx"
        ON
            "{schemaName}"."{self.name}"
        USING GiST
            ( coord
            )
        {indexSpace}
        WHERE
            coord IS NOT NULL
            AND isprimary
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
        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_coord_idx"
        """.format(**locals())
        )

        # indices WHERE isprimary = True

        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_object_id_primary_idx"
        """.format(**locals())
        )
        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_skymap_id_primary_idx"
        """.format(**locals())
        )
        cursor.execute("""
        DROP INDEX IF EXISTS
            "{schemaName}"."{self.name}_coord_primary_idx"
        """.format(**locals())
        )

def get_catalog_schema_from_file(path, object_id):
    """
    Get fields in a "forced_src-*.fits" file.
    @param path
        Path to a "forced-*.fits" file
    @param object_id
        numpy.array of object ID from the corresponding "forced-*.fits" file.
    @return
        PoppingOrderedDict mapping name: str -> table: DBTable.
    """

    #print("getting catalog schema")
    table = lib.sourcetable.SourceTable.from_hdu(lib.fits.fits_open(path)[1])

    these_object_id = table.cutout_subtable("id").fields["id"].data

    if not numpy.all(these_object_id == object_id):
        raise RuntimeError("object_id in forced_src doesn't agree with ref " + path)

    algos = PoppingOrderedDict(
        (name, algoclass(table))
        for name, algoclass in lib.forced_algos.forced_algos.items()
    )

    # To suppress warnings "Ignored field: ...",
    # ignore the fields explicitly.
    def ignore(prefix):
        table.cutout_subtable(prefix)

    for name in lib.forced_algos.forced_algos_ignored:
        ignore(name)

    for field in table.fields:
        lib.misc.warning('Ignored field: ', field, 'in', path)

    dbtables = PoppingOrderedDict()
    def add(name, sourcenames, dbtable_class=lib.dbtable.DBTable):
        dbtables[name] = dbtable_class(name, algos.pop_many(sourcenames))

    add("_forced:part1", [
        "base_PixelFlags",
        "base_InputCount",
        "base_Variance",
        "base_LocalBackground",
        "base_ClassificationExtendedness",    # not in lsst 1.1 data
        "modelfit_CModel",
    ])

    add("_forced:part2", [
        "base_SdssCentroid",
        "base_GaussianFlux",
        "base_PsfFlux",
        "ext_photometryKron_KronFlux",   # not in lsst 1.1 data
        "base_SdssShape",
        "modelfit_DoubleShapeletPsfApprox",   # not in lsst 1.1 data
        "undeblended_base_PsfFlux",
        "undeblended_ext_photometryKron_KronFlux",  # not in lsst 1.1 data
    ])

    add("_forced:part3", [
        "base_CircularApertureFlux",
        "undeblended_base_CircularApertureFlux",   # not in lsst 1.1 data
    ])

    # Put this back in for Run1.2
    # NOTE:  This code will no longer work for 1.1
    add("_forced:part4", [
        "ext_convolved_ConvolvedFlux",
    ])

    if algos:
        print("remaining algos:")
        print(type(algos))
        for k in algos: print(str(k))
        raise RuntimeError("Algorithms remain unused. See above. ")

    return dbtables

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
        for path in glob.iglob("{rerunDir}/deepCoadd-results/merged/{tract}/*,*/ref-*".format(**locals()))
        if path.endswith('.fits') or path.endswith('.fits.gz')
    )

    return sorted(set(
        lib.common.patch_to_number(patch) for patch in patches if re.match(r'^[0-9]+,[0-9]+$', patch)
    ))

def get_ref_path(rerunDir, tract, patch):
    """
    Get the path to the "ref-*.fits" catalog identified by the arguments.
    The file may be compressed, but the path is virtualized so it always ends with '.fits'.
    """
    if patch == "*":
        x, y = "*", "*"
    else:
        x, y = patch // 100, patch % 100
    return "{rerunDir}/deepCoadd-results/merged/{tract}/{x},{y}/ref-{tract}-{x},{y}.fits".format(**locals())

def get_catalog_path(rerunDir, tract, patch, filter, hsc=True, schemaName=None):
    """
    Get the path to the "forced_src-*.fits" catalog identified by the arguments.
    The file may be compressed, but the path is virtualized so it always ends with '.fits'.
    """
    if patch == "*":
        x, y = "*", "*"
    else:
        x, y = patch // 100, patch % 100
    if hsc is True:
        return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/forced_src-{filter}-{tract}-{x},{y}.fits".format(**locals())
    else:
        (major, minor, sim_type) = extract_schema_fields(schemaName)
        if major == None or (str(major) == '1' and str(minor) == '1'):
            return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/forced-{filter}-{tract}-{x},{y}.fits".format(**locals())
        else:
            return "{rerunDir}/deepCoadd_results/{filter}_t{tract}_p{x},{y}/forced-{filter}-{tract}-{x},{y}.fits".format(**locals())
def get_an_existing_catalog_id(rerunDir, schemaName):
    """
    Get any one triple (tract, patch, filter) for which catalog files exist
    """
    pattern = get_catalog_path(rerunDir, "*", "*", "*", False, schemaName)

    for catPath in itertools.chain(glob.iglob(pattern), glob.iglob(pattern + ".gz")):
        #print('catPath is: ', catPath)
        tract, patch, filter = lib.common.path_decompose(catPath)
        #print('tract={tract}, patch={patch}, filter={filter}'.format(**locals()))
        refPath = get_ref_path(rerunDir, tract, patch)
        #print('refPath is {refPath}'.format(**locals()))
        if lib.common.path_exists(refPath):
            return tract, patch, filter

    raise RuntimeError("No complete pair (ref, forced_src) exists.")


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


    if cursor is None:  return False

    # The files that need registering include a "ref" file as well as multiband files.
    # We address this problem by giving filter ID 0  (or file_id minFileId) to the "ref" file,
    # and letting actual filter IDs start with 1.
    fileId = [minFileId] + sorted(patchId*100 + lib.common.filterOrder[f]+1 for f in filters)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS "{schemaName}"."_temp:forced_patch" (
        file_id   Bigint   PRIMARY KEY
    )
    """.format(**locals())
    )

    cursor.execute("""
    SELECT file_id FROM "{schemaName}"."_temp:forced_patch" WHERE
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
    INSERT INTO "{schemaName}"."_temp:forced_patch"
    VALUES {sFileId}
    """.format(**locals())
    )

    return False

def extract_schema_fields(schemaName):
    """
    Expecting input of the form alphastringDDS   where D is major version 
    (single digit), D is minor version (one or more digits) and 
    S is simulator type (one of 'p' or 'i').   Return a triple
    (major-version, minor-version, sim-type)
    @param schemaName
    """
    if schemaName is None: 
        return(None, None, None)

    pat = '\A[-_a-zA-Z]+([0-9])([0-9]+)(i|p)\Z'
    result = re.match(pat, schemaName)
    if result:
        return(result.group(0), result.group(1), result.group(2))
    else:
        return(None, None, None)
if __name__ == "__main__":
    main()
