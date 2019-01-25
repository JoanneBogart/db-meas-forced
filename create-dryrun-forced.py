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
from lib.dpdd import DpddView

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
    cmdline = ' '.join(sys.argv)
    print('Invocation: ' + cmdline)
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Read a rerun directory to create "forced" summary table.')

    parser.add_argument('rerunDir', 
                        help="Rerun directory from which to read data")
    parser.add_argument('schemaName', 
                        help="DB schema name in which to load data")
    parser.add_argument("--table-name", default="forced", 
                        help="Top-level table's name")
    parser.add_argument("--table-space", default="", 
                        help="DB table space for tables")
    parser.add_argument("--index-space", default="", 
                        help="DB table space for indexes")
    parser.add_argument("--db-server", metavar="key=value", nargs="+", 
                        action="append", 
                        help="DB connect parms. Must come after reqd args.")
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
    parser.add_argument('--tracts', dest='tracts', type=int, nargs='+', 
                        help="Ingest data for specified tracts only if present. Else ingest all")
    parser.add_argument('--imageRerunDir', default=None, 
                        help="Root dir for finding images; defaults to rerunDir")
    args = parser.parse_args()

    if args.tracts is not None:
        print("Processing the following tracts:")
        for t in args.tracts: print(t)

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
                                         args.table_name, filters, args.dryrun,
                                         args.imageRerunDir)
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
                                     filters, dryrun, imageRerunDir):
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
    bNeedView = False

    db = lib.common.new_db_connection()

    with db.cursor() as cursor:
        try:
            cursor.execute('SELECT 0 FROM "{schemaName}"."position" WHERE FALSE;'.format(**locals()))
        except psycopg2.ProgrammingError:
            bNeedCreating = True
            db.rollback()

    # Now check for view
    with db.cursor() as cursor:
        try:
            cursor.execute('SELECT 0 FROM "{schemaName}"."dpdd" WHERE FALSE;'.format(**locals()))
        except psycopg2.ProgrammingError:
            bNeedView = True
            db.rollback()

    if bNeedView:
        print("view needs creating")
    else:
        print("View is already there")
        
    create_schema_string = 'CREATE SCHEMA IF NOT EXISTS "{schemaName}"'.format(**locals())
    dm_schema = 1
    if (not dryrun):
        if bNeedCreating:
            with db.cursor() as cursor:
                cursor.execute(create_schema_string)
                dm_schema = create_mastertable(cursor, rerunDir, schemaName, 
                                               masterTableName, filters,
                                               imageRerunDir)
                create_view(cursor, schemaName, dm_schema)
            db.commit()
        else:
            if bNeedView:
                tract, patch, filter = get_an_existing_catalog_id(rerunDir, 
                                                                  schemaName)
                refPath = get_ref_path(rerunDir, tract, patch)
                universals,object_id,coord,dm_schema = get_ref_schema_from_file(refPath)
                if dm_schema is None:
                    print("Cannot determine dm schema. Bailing..")
                    return

                with db.cursor() as cursor:
                    create_view(cursor, schemaName, dm_schema)
                db.commit()
            else:
                db.close()
                drop_index_from_mastertable(rerunDir, schemaName, filters)
    else:
        if bNeedCreating:
            print("Would execute: ")
            print(create_schema_string)
            cursor = None
            dm_schema = create_mastertable(cursor, rerunDir, schemaName, 
                                           masterTableName, filters, 
                                           imageRerunDir)
            create_view(cursor, schemaName, dm_schema)
        else:
            print("Master table already exists")
            print("pretend create anyway:")
            print(create_schema_string)
            cursor = None
            dm_schema = create_mastertable(cursor, rerunDir, schemaName, 
                                           masterTableName, filters, 
                                           imageRerunDir)
            create_view(cursor, schemaName, dm_schema)
def create_mastertable(cursor, rerunDir, schemaName, masterTableName, filters,
                       imageRerunDir):
    """
    Create the tables

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

    if imageRerunDir == None: imageRerunDir = rerunDir
    tract, patch, filter = get_an_existing_catalog_id(rerunDir, schemaName)
    catPath = get_catalog_path(rerunDir, tract, patch, filter, hsc=False,
                               schemaName=schemaName)
    refPath = get_ref_path   (rerunDir, tract, patch)

    universals,object_id,coord,dm_schema = get_ref_schema_from_file(refPath)
    if dm_schema is None: dm_schema = 1
    multibands = get_catalog_schema_from_file(catPath, object_id)

    for table in itertools.chain(universals.values(), multibands.values()):
        table.set_filters(filters)

    for table in itertools.chain(universals.values(), multibands.values()):
        #if ('position' in table.name):
        #    print("About to transform table ", table.name)
        table.transform(imageRerunDir, tract, patch, filter, coord)

    # Create source tables
    for table in itertools.chain(universals.values(), multibands.values()):
        table.create(cursor, schemaName)

    return dm_schema

    #  OMIT old view code,including table comment. We have no old-style views


def create_view(cursor, schemaName, dm_schema):
    """
    Creates dpdd view.
    @param cursor
       cursor for writing to db.  If None, just print
    @param schemaName
       Name of schema in which to locate the view
    @param dm_schema
       dm table schema version used to produce the data.  Naming conventions
       for native quantities vary somewhat depending on this version

    """
    yaml_path = os.path.join(os.getenv('DPDD_YAML'),'native_to_dpdd.yaml')
    yaml_override = os.path.join(os.getenv('DPDD_YAML'),
                                 'postgres_override.yaml')
    view_builder = DpddView(schemaName, yaml_path=yaml_path,
                            yaml_override=yaml_override,
                            dm_schema_version=int(dm_schema))
    vs = view_builder.view_string()
    if cursor:
        cursor.execute(vs)
    else:
        print("Create view command would be:")
        print(vs)


def insert_into_mastertable(rerunDir, schemaName, masterTableName, filters,
                            dryrun, tracts):
    """
    Insert data into tables.
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
        universals,object_id,coord,dm_schema = get_ref_schema_from_file(refPath)

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

    universals, object_id, coord, dm_schema = get_ref_schema_from_file(refPath)
    multibands = get_catalog_schema_from_file(catPath, object_id)

    for table in itertools.chain(universals.values(), multibands.values()):
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in itertools.chain(universals.values(), multibands.values()):
            if ('position' in table.name):
                table.set_dbconnection(db)
                table.create_index(cursor, schemaName)
                table.set_dbconnection(None)
            else:
                table.create_index(cursor, schemaName)
                db.commit()
    #db.commit()


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

    universals, object_id, coord, dm_schema = get_ref_schema_from_file(refPath)
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
        * "dm_schema_version" Value of 'AFW_TABLE_VERSION' keyword
    """
    table = lib.sourcetable.SourceTable.from_hdu(lib.fits.fits_open(path)[1])

    dm_schema_version = table.dm_schema_version()

    # All fields starting with 'id' are removed from source table 'table'
    # and put in a new table.  In practice this table has a single column
    # named 'object_id'
    object_id = table.cutout_subtable("id").fields["id"].data

    algos = PoppingOrderedDict(
        (name, algoclass(table))
        for name, algoclass in lib.forced_algos.ref_algos.items()
    )

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
    def add(name, sourcenames, 
            dbtable_class=lib.dbtable.DBTable_BandIndependent):
        dbtables[name] = dbtable_class(name, algos.pop_many(sourcenames))

    #   New table names:
    #       position (= old _forced:position)
    #       dpdd_ref (remaining quantitites from ref used in dpdd)
    #       misc_ref  (everything else in ref)
    #       dpdd_forced (multiband quantities in dpdd)
    #       forced2
    #       forced3
    #       forced4
    #       forced5
    #   view dpdd will come from position join dpdd_ref join dpdd_forced

    add("position", [
        "ref_coord", "detect", "merge"
    ], dbtable_class=DBTable_Position)
    add("dpdd_ref",
        ["base_SdssCentroid", "base_PsfFlux","base_ClassificationExtendedness",
         "base_Blendedness","base_PixelFlags", "ext_shapeHSM", 
         "base_SdssShape", "modelfit_CModel", "deblend", ])
    add("misc_ref",
        ["base_CircularApertureFlux",
         "base_FootprintArea",
         "base_GaussianCentroid",
         "base_GaussianFlux",
         "base_InputCount",
         "base_LocalBackground",
         "base_NaiveCentroid",
         "base_Variance",
         "calib",
         "ext_convolved_ConvolvedFlux",
         "ext_photometryKron_KronFlux",
         "footprint",
         "modelfit_DoubleShapeletPsfApprox",])

    if algos:
        print("remaining algos:")
        print(type(algos))
        for k in algos: print(str(k))
        raise RuntimeError("Algorithms remain unused; see above ")

    return dbtables, object_id, coord, dm_schema_version

# Changes to accommodate leaving field 'parent' as is (no change to 'parent_id')
class DBTable_Position(lib.dbtable.DBTable_BandIndependent):
    def __init__(self, name, algos):
        super().__init__(name, algos)
        self.dbconn = None

    def set_dbconnection(self, dbconn):
        self.dbconn = dbconn

    def create_index(self, cursor, schemaName):
        lib.dbtable.DBTable_BandIndependent.create_index(self, cursor, schemaName)
        indexSpace = lib.config.get_index_space()

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS
            "{self.name}_parent_id_idx"
        ON
            "{schemaName}"."{self.name}"
            ( parent
            )
        {indexSpace}
        """.format(**locals())
        )
        if (self.dbconn): self.dbconn.commit()
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS
            "{self.name}_skymap_id_idx"
        ON
            "{schemaName}"."{self.name}"
            ( public.skymap_from_object_id(object_id)
            )
        {indexSpace}
        """.format(**locals())
        )
        if (self.dbconn): self.dbconn.commit()
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS
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
        if (self.dbconn): self.dbconn.commit()

        # indices WHERE detect_isprimary = True
        cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
            "{self.name}_object_id_primary_idx"
        ON
            "{schemaName}"."{self.name}"
            ( object_id
            )
        {indexSpace}
        WHERE
          detect_isprimary
        """.format(**locals())
        )
        if (self.dbconn): self.dbconn.commit()
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS
            "{self.name}_skymap_id_primary_idx"
        ON
            "{schemaName}"."{self.name}"
            ( public.skymap_from_object_id(object_id)
            )
        {indexSpace}
        WHERE
          detect_isprimary
        """.format(**locals())
        )
        if (self.dbconn): self.dbconn.commit()
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS
            "{self.name}_coord_primary_idx"
        ON
            "{schemaName}"."{self.name}"
        USING GiST
            ( coord
            )
        {indexSpace}
        WHERE
            coord IS NOT NULL
            AND detect_isprimary
        """.format(**locals())
        )
        if (self.dbconn): self.dbconn.commit()

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

        # indices WHERE detect_isprimary = True

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
        numpy.array of object ID from the corresponding "ref-*.fits" file.
    @return
        PoppingOrderedDict mapping name: str -> table: DBTable.
    """

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

    add("dpdd_forced", [
        "base_PixelFlags",
        "base_InputCount",
        "base_Variance",
        "base_LocalBackground",
        "base_ClassificationExtendedness",    # not in lsst 1.1 data
        "modelfit_CModel",
        "base_SdssCentroid",
        "base_SdssShape",
        "base_PsfFlux",
    ])

    add("forced2", [
        "base_GaussianFlux",
        "ext_photometryKron_KronFlux",   # not in lsst 1.1 data
        "modelfit_DoubleShapeletPsfApprox",   # not in lsst 1.1 data
        "undeblended_base_PsfFlux",
        "undeblended_ext_photometryKron_KronFlux",  # not in lsst 1.1 data
    ])

    add("forced3", [
        "base_CircularApertureFlux",
        "undeblended_base_CircularApertureFlux",   # not in lsst 1.1 data
        "base_TransformedCentroid",
        "base_TransformedShape",
        "multi_coord",
    ])

    # Put this back in for Run1.2
    # NOTE:  This code will no longer work for 1.1
    add("forced4", [
        "ext_convolved_ConvolvedFlux",
    ])

    add("forced5", [
        "undeblended_ext_convolved_ConvolvedFlux",
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
        #print('extract_schema_fields found major={major}, minor={minor}'.format(**locals()))
        if major == None or (str(major) == '1' and str(minor) == '1'):
            return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/forced-{filter}-{tract}-{x},{y}.fits".format(**locals())
        else:
            return "{rerunDir}/deepCoadd_results/{filter}_t{tract}_p{x},{y}/forced-{filter}-{tract}-{x},{y}.fits".format(**locals())
def get_an_existing_catalog_id(rerunDir, schemaName):
    """
    Get any one triple (tract, patch, filter) for which catalog files exist
    """
    pattern = get_catalog_path(rerunDir, "*", "*", "*", False, schemaName)
    #print('pattern from get_catalog_path is {pattern}'.format(**locals()))

    for catPath in itertools.chain(glob.iglob(pattern), glob.iglob(pattern + ".gz")):
        tract, patch, filter = lib.common.path_decompose(catPath)

        refPath = get_ref_path(rerunDir, tract, patch)
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

    # The files that need registering include a "ref" file as well as 
    # multiband files.
    # We address this problem by giving filter ID 0  (or file_id minFileId) 
    # to the "ref" file, and letting actual filter IDs start with 1.
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
    Optionally allow _ followed by a "word" (all lower case letters)
    @param schemaName
    """
    if schemaName is None: 
        return(None, None, None)

    pat = '\A[-_a-zA-Z]+([0-9])([0-9]+)(i|p)(_[a-z]+[a-z,0-9]*)?\Z'
    result = re.match(pat, schemaName)
    if result:
        return(result.group(1), result.group(2), result.group(3))
    else:
        return(None, None, None)
if __name__ == "__main__":
    main()
