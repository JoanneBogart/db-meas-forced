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
import lib.random_algos
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
import warnings


def main():
    import argparse
    parser = argparse.ArgumentParser(
        fromfile_prefix_chars='@',
        description='Read a rerun directory to create "random" summary table.')

    parser.add_argument('rerunDir', help="Rerun directory from which to read data")
    parser.add_argument('schemaName', help="DB schema name in which to load data")

    parser.add_argument("--table-name", default="random", help="Top-level table's name")
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

    args = parser.parse_args()

    if args.db_server:
        lib.config.dbServer.update(keyvalue.split('=', 1) for keyvalue in itertools.chain.from_iterable(args.db_server))

    lib.config.tableSpace = args.table_space
    lib.config.indexSpace = args.index_space
    lib.config.withSkymapWcs = args.with_skymap_wcs

    filters = lib.common.get_existing_filters(args.rerunDir)
    if args.create_index:
        create_index_on_mastertable(args.rerunDir, args.schemaName, filters)
    else:
        create_mastertable_if_not_exists(args.rerunDir, args.schemaName, args.table_name, filters)
        insert_into_mastertable(args.rerunDir, args.schemaName, args.table_name, filters)


def create_mastertable_if_not_exists(rerunDir, schemaName, masterTableName, filters):
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

    if bNeedCreating:
        with db.cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS "{schemaName}"'.format(**locals()))
            create_mastertable(cursor, rerunDir, schemaName, masterTableName, filters)
        db.commit()
    else:
        db.close()
        drop_index_from_mastertable(rerunDir, schemaName, filters)


def create_mastertable(cursor, rerunDir, schemaName, masterTableName, filters):
    """
    Create the master table.
    The master table will actually be a view, which JOINs (not UNIONs) child tables.
    The child tables will also be created during a call to this function.

    This function only creates these tables. It does not insert data into them.

    @param cursor
        DB connection's cursor object
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
    multibands, object_id, coord = get_catalog_schema_from_file(catPath, None)

    for table in multibands.values():
        table.set_filters(filters)

    for table in multibands.values():
        table.transform(rerunDir, tract, patch, filter, coord)

    # Create source tables
    for table in multibands.values():
        table.create(cursor, schemaName)

    # Create master table
    commentOnTable = """
    Catalog of random objects.
    </p><p>
    Notice that the <code>object_id</code> in this table do not agree with those in the other tables including
    <code>forced</code>, <code>meas</code> and <code>specz</code>. This means it is totally nonsense to join
    <code>random</code> to these tables: <s><code>forced JOIN random USING(object_id)</code></s>, for example.
    """

    # Because the number of columns exceeds PostgreSQL's limit,
    # we divide the master table into "random", "random2", "random3", ...

    listMultibands = [ multibands ]
    # listMultibands.append(multibands.pop_many([
    #     "...", "...", ...
    # ]))

    for iPart, multibands in enumerate(listMultibands, start=1):
        dbTables = [table.name for table in multibands.values()]
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
        for table in multibands.values():
            fieldDefs += table.get_bandindependent_exported_fields()
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

        cursor.execute("""
        CREATE VIEW "{schemaName}"."{tableName}" AS (
            SELECT
                {sFieldDefs}
            FROM
                {dbTables}
        )
        """.format(**locals())
        )

        for name, definition, unit, doc in fieldDefs:
            cursor.execute("""
            COMMENT ON COLUMN "{schemaName}"."{tableName}"."{name}" IS %(comment)s
            """.format(**locals()), dict(comment = "{doc} || {unit}".format(**locals()))
            )

        cursor.execute("""
        COMMENT ON VIEW "{schemaName}"."{tableName}" IS %(comment)s
        """.format(**locals()), dict(comment = commentOnTable)
        )

def insert_into_mastertable(rerunDir, schemaName, masterTableName, filters):
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
    """
    for tract in lib.common.get_existing_tracts(rerunDir):
        for patch in get_existing_patches(rerunDir, tract):
            insert_patch_into_mastertable(rerunDir, schemaName, masterTableName, filters, tract, patch)


def insert_patch_into_mastertable(rerunDir, schemaName, masterTableName, filters, tract, patch):
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
        if is_patch_already_inserted(cursor, schemaName, tract, patch, catPaths.keys()):
            lib.misc.warning("Skip because already inserted: (tract,patch) = ({tract}, {patch})".format(**locals()))
            return

        object_id = None
        multibands = {}
        for filter, catPath in catPaths.items():
            if object_id is None:
                mult, object_id, coord = get_catalog_schema_from_file(catPath, None)
            else:
                mult, dummy, dummy2 = get_catalog_schema_from_file(catPath, object_id)

            for table in mult.values():
                table.transform(rerunDir, tract, patch, filter, coord)

                if table.name not in multibands:
                    multibands[table.name] = []
                multibands[table.name].append((table, filter))

        for tables in multibands.values():
            insert_patch_into_multibandtable(cursor, schemaName, tables, object_id)

    db.commit()


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

    for name, fmt, cols in tables[0][0].get_bandindependent_backend_field_data():
        columns.extend(cols)
        fieldNames.append(name)
        format += "\t" + fmt

    for table, filter in tables:
        for name, fmt, cols in table.get_backend_field_data(filter):
            columns.extend(cols)
            fieldNames.append(name)
            format += "\t" + fmt

    format += "\n"
    format = format.encode("utf-8")

    if lib.config.MULTICORE:
        fin = pipe_printf.open(format, *columns)
        cursor.copy_from(fin, '"{}"."{}"'.format(schemaName, table.name), sep='\t', columns=fieldNames)
    else:
        tsv = b''.join(format % tpl for tpl in zip(*columns))
        fin = io.BytesIO(tsv)
        cursor.copy_from(fin, '"{}"."{}"'.format(schemaName, table.name), sep='\t', size=-1, columns=fieldNames)


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

    multibands, object_id, coord = get_catalog_schema_from_file(catPath, None)

    for table in multibands.values():
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in multibands.values():
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

    multibands, object_id, coord = get_catalog_schema_from_file(catPath, None)

    for table in multibands.values():
        table.set_filters(filters)

    db = lib.common.new_db_connection()
    with db.cursor() as cursor:
        for table in multibands.values():
            table.drop_index(cursor, schemaName)
    db.commit()


def get_catalog_schema_from_file(path, object_id):
    """
    Get fields in a "ran-*.fits" file.
    @param path
        Path to a "ran-*.fits" file
    @param object_id
        numpy.array of object_id. Optional.
    @return (multibands, object_id, coord)
        * multibands: PoppingOrderedDict mapping name: str -> table: DBTable
        * object_id: numpy.array of object_id.
        * coord: {"ra": numpy.array, "dec": numpy.array},
            in which angles are in degrees.
    """
    table = lib.sourcetable.SourceTable.from_hdu(lib.fits.fits_open(path)[1])

    these_object_id = table.cutout_subtable("id").fields["id"].data

    if (object_id is not None) and (not numpy.all(these_object_id == object_id)):
        raise RuntimeError("object_id in ran-*.fits doesn't agree with another filter" + path)

    algos = PoppingOrderedDict(
        (name, algoclass(table))
        for name, algoclass in lib.random_algos.random_algos.items()
    )

    coord = algos["random_coord"].coord

    # To suppress warnings "Ignored field: ...",
    # ignore the fields explicitly.
    def ignore(prefix):
        table.cutout_subtable(prefix)

    for name in lib.random_algos.random_algos_ignored:
        ignore(name)

    for field in table.fields:
        lib.misc.warning('Ignored field: ', field, 'in', path)

    dbtables = PoppingOrderedDict()
    dbtables["_random:random"] = DBTable_Random("_random:random", algos)

    return dbtables, these_object_id, coord


class DBTable_Random(lib.dbtable.DBTable):
    def create_index(self, cursor, schemaName):
        lib.dbtable.DBTable.create_index(self, cursor, schemaName)

        indexSpace = lib.config.get_index_space()

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
        lib.dbtable.DBTable.drop_index(self, cursor, schemaName)

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

    def create(self, cursor, schemaName):
        """
        Create table in the database.
        A primary key "object_id" will be included
        regardless of whether it exists in self.fields or not.
        @param cursor
            DB connection's cursor object
        @param schemaName
            Name of the schema in which to locate the master table
        """
        members = ["object_id Bigint"]

        # band-independent fields
        for name, type in self.algos['random_coord'].get_backend_fields(""):
            members.append("{name}  {type}".format(**locals()))

        # multiband fields
        for filter in self.filters:
            filt = lib.common.filterToShortName[filter] + "_" if filter else ""
            for key, algo in self.algos.items():
                if key == 'random_coord': continue
                for name, type in algo.get_backend_fields(filt):
                    members.append("{name}  {type}".format(**locals()))

        members = """,
        """.join(members)

        tableSpace = lib.config.get_table_space()

        cursor.execute("""
        CREATE TABLE "{schemaName}"."{self.name}" (
            {members}
        )
        {tableSpace}
        """.format(**locals())
        )

    def get_bandindependent_backend_field_data(self):
        """
        Get band-independent field data for the backend table.
        @return list of (fieldname, printf_format, [column]).
            'column' is a numpy.array. An example of the return value is:
            ("i_point", "(%.16e,%.16e)", [x, y]),
            in which x and y are numpy.array.
        """
        return self.algos['random_coord'].get_backend_field_data("")

    def get_backend_field_data(self, filter):
        """
        Get field data for the backend table.
        @param filter (str)
            Filter name.
        @return list of (fieldname, printf_format, [column]).
            'column' is a numpy.array. An example of the return value is:
            ("i_point", "(%.16e,%.16e)", [x, y]),
            in which x and y are numpy.array.
        """
        filt = lib.common.filterToShortName[filter] + "_" if filter else ""
        members = []

        for key, algo in self.algos.items():
            if key == 'random_coord': continue
            members += algo.get_backend_field_data(filt)

        return members

    def get_bandindependent_exported_fields(self):
        """
        @return list of (fieldname, definition, unit, document).
            Each field can be exported as:
                {definition} AS {fieldname}.
        """
        return self.algos['random_coord'].get_frontend_fields("")

    def get_exported_fields(self, filter):
        """
        Get field data for the frontend view.
        @param filter (str)
            Filter name.
        @return list of (fieldname, definition, unit, document).
            Each field can be exported as:
                {definition} AS {fieldname}.
        """
        filt = lib.common.filterToShortName[filter] + "_" if filter else ""
        members = []

        for key, algo in self.algos.items():
            if key == 'random_coord': continue
            members += algo.get_frontend_fields(filt)

        return members


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
        for path in glob.iglob("{rerunDir}/deepCoadd-results/*/{tract}/*,*/ran-*.fits*".format(**locals()))
        if path.endswith('.fits') or path.endswith('.fits.gz')
    )

    return sorted(set(
        lib.common.patch_to_number(patch) for patch in patches if re.match(r'^[0-9]+,[0-9]+$', patch)
    ))


def get_catalog_path(rerunDir, tract, patch, filter):
    """
    Get the path to the "ran-*.fits" catalog identified by the arguments.
    The file may be compressed, but the path is virtualized so it always ends with '.fits'.
    """
    if patch == "*":
        x, y = "*", "*"
    else:
        x, y = patch // 100, patch % 100
    return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/ran-{filter}-{tract}-{x},{y}.fits".format(**locals())

def get_an_exisiting_catalog_id(rerunDir):
    """
    Get any one triple (tract, patch, filter) for which catalog files exist
    """
    pattern = get_catalog_path(rerunDir, "*", "*", "*")

    for imagePath in itertools.chain(glob.iglob(pattern), glob.iglob(pattern + ".gz")):
        tract, patch, filter = lib.common.path_decompose(imagePath)
        return tract, patch, filter

    raise RuntimeError("No catalog (ran-*.fits) exists.")


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
    CREATE TABLE IF NOT EXISTS "{schemaName}"."_temp:random_patch" (
        file_id   Bigint   PRIMARY KEY
    )
    """.format(**locals())
    )

    cursor.execute("""
    SELECT file_id FROM "{schemaName}"."_temp:random_patch" WHERE
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
    INSERT INTO "{schemaName}"."_temp:random_patch"
    VALUES {sFileId}
    """.format(**locals())
    )

    return False


if __name__ == "__main__":
    main()
