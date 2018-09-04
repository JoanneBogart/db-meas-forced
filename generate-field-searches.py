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

import psycopg2

import itertools
import os
import pickle
import re
import sys
import textwrap

default_db_server = {
    'dbname': os.environ.get("USER", "postgres"),
}


# [schema][field] = representative tract
g_fieldNames = {
    "pdr1_udeep": {
        "cosmos": 9813,
        "sxds": 8523,
    },
    "pdr1_deep": {
        "cosmos": 9813,
        "deep2_3": 9464,
        "elais_n1": 17130,
        "xmm_lss": 8524,
    },
    "pdr1_wide": {
        "wide01h": 9482,
        "xmm_lss": 8524,
        "gama09h": 9560,
        "wide12h": 9348,
        "gama15h": 9372,
        "hectomap": 16010,
        "vvds": 9696,
        "aegis": 16973,
    },
    "s17a_dud": {
        "cosmos"  : 9813,
        "deep2_3" : 9464,
        "elais_n1": 17130,
        "sxds"    : 8523,
    },
    "s17a_wide": {
        "w01": 9482,
        "w02": 8524,
        "w03": 9560,
        "w04": 9348,
        "w05": 9696,
        "w06": 16010,
        "w07": 16973,
    },
}

g_aliases = {
    "s17a_dud": {
        "xmm_lss": "sxds",
    },
    "s17a_wide": {
        "wide01h" : "w01",
        "xmm_lss" : "w02",
        "gama09h" : "w03",
        # "wide12h"+"gama15h" -> "w04",
        "vvds"    : "w05",
        "hectomap": "w06",
        "aegis"   : "w07",
    },
}


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
        description='Generate field search functions.')

    parser.add_argument(
        "--db-server",
        metavar="key=value",
        nargs="+",
        default=[],
        action="append",
        help="DB to connect to. This option must come later than non-optional arguments.",
    )

    parser.add_argument(
        "--format",
        choices=["sql", "json", ],
        default="sql",
        help="Print format.",
    )

    return parser


def main(db_server, format="sql"):
    graph = pickle.load(open("tractGraph.pickle"))

    with Printer.create(format, sys.stdout) as printer:
        for schema in ["s17a_dud", "s17a_wide"]:
            tracts = download_existing_tracts(db_server, schema)
            groups = group_tracts(tracts, graph)
            found_fields = set()

            for group in groups:
                fieldnames = get_fieldnames(schema, group)
                found_fields.update(fieldnames)
                if not fieldnames:
                    printer.warning("No field name.")
                    fieldname = ""
                elif len(fieldnames) == 1:
                    fieldname = fieldnames[0]
                else:
                    printer.warning("Field name is not unique: " + ", ".join(fieldnames))
                    fieldname = fieldnames[0]

                printer.print_group(schema, fieldname, group)

            for alias, fieldname in g_aliases[schema].items():
                if fieldname in found_fields:
                    printer.print_alias(schema, alias, fieldname)


def download_existing_tracts(db_server, schema):
    """
    Get list of tracts existing in a schema.
    @param db_server (dict): conninfo (cf. libpq's PQconnectdb()) of postgres.
    @param schema (str): schema name
    @return: list of tracts as integers.
    """
    conn = psycopg2.connect(**db_server)

    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        tract
    FROM
        generate_series(0, 32767) AS candidate(tract)
    WHERE
        EXISTS(SELECT object_id FROM {schema}.forced
            WHERE tractSearch(object_id, candidate.tract)
        )
        OR
        EXISTS(SELECT object_id FROM {schema}.meas
            WHERE tractSearch(object_id, candidate.tract)
        )
    """.format(**locals()))

    return [tract for tract, in cursor]


def group_tracts(tracts, graph):
    """
    Group tracts into adjacent regions.
    @param tracts (list of integers)
    @param graph (list of list of integers)
        'group[tract]' is the list of the tracts adjacent to the 'tract'.
    @return (list of set of integers)
        Each set of tracts as integers will be an adjacent region.
    """
    # 'groups' will be a list of (tracts, neighbors)
    #   where tracts = set of tracts in the group
    #   and neighbors = set of tracts in the neighborhood of the group.
    groups = []
    for tract in tracts:
        # Select existing groups in whose neighborhood is the focused tract
        selectedGroups = set(
            iGroup for iGroup, (tracts, neighbors) in enumerate(groups)
            if tract in neighbors
        )

        # The selected groups are united by the focused tract.
        # Let us create the union of the tracts in the selected groups.
        unionTracts = set([tract])
        unionNeighbors = set(graph[tract])
        for iGroup in selectedGroups:
            tracts, neighbors = groups[iGroup]
            unionTracts    |= tracts
            unionNeighbors |= neighbors

        # Delete the selected groups from 'groups' since they are now united.
        groups = [
            group for iGroup, group in enumerate(groups)
            if iGroup not in selectedGroups
        ]

        # Add the newly created group to 'groups'
        groups.append((unionTracts, unionNeighbors))

    return [tracts for tracts, neighbors in groups]


def get_fieldnames(schema, tracts):
    """
    Get field names for a list of tracts.
    In the normal case, the length of the returned list is 1.
    It should be treated as error
    if no name found or more than one name found.

    @param schema (str)
    @param tracts (list of integers)
    @return (list of str)
    """

    tracts = set(tracts)

    return [field for field, representativeTract in g_fieldNames[schema].items()
        if representativeTract in tracts]


class Printer(object):
    dictionary = {}

    @classmethod
    def create(cls, name, *args, **kwargs):
        return cls.dictionary[name](*args, **kwargs)

    @classmethod
    def register(cls, name):
        def registerer(product_class):
            cls.dictionary[name] = product_class
            return product_class

        return registerer


@Printer.register("sql")
class SQLPrinter(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def __enter__(self):
        self.fileobj.write('BEGIN;\n')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.fileobj.write('COMMIT;\n')
        else:
            self.fileobj.write('ROLLBACK;\n')

    def print_group(self, schema, fieldname, tracts):
        """
        Print a group of tracts as an SQL search function.
        @param schema (str)
        @param fieldname (str)
        @param tracts (list of integers)
        """
        self.fileobj.write(textwrap.dedent("""
            CREATE OR REPLACE FUNCTION
              {schema}.search_{fieldname}
              ( IN   object_id  Bigint
              , OUT  isIn       Boolean
              )
            LANGUAGE SQL
            IMMUTABLE
            AS $$
              SELECT
        """).lstrip().format(**locals()))

        self.fileobj.write(
            '    ' +
            ' OR\n    '.join(
                '(object_id BETWEEN {first_id} AND {last_id}) /* {first} <= tract <= {last} */'
                .format(
                    first    = first,
                    last     = last,
                    first_id = first * (2**42),
                    last_id  = (last+1) * (2**42) - 1,
                )
                for first, last in list_to_spans(tracts)
            ) +
            ' ;\n'
        )

        self.fileobj.write(textwrap.dedent("""
            $$;
            COMMENT ON FUNCTION {schema}.search_{fieldname}(Bigint) IS
            $$Return true if the object is in field '{fieldname}'.$$;

        """).lstrip().format(**locals()))

        # Warn about abandoned API
        self.fileobj.write(textwrap.dedent("""
            CREATE OR REPLACE FUNCTION
              {schema}.search_{fieldname}
              ( IN   skymap_id  Integer
              , OUT  isIn       Boolean
              )
            LANGUAGE PLPGSQL
            IMMUTABLE
            AS $$
              BEGIN
                  RAISE EXCEPTION 'search_{fieldname}(skymap_id, ...) has been replaced by search_{fieldname}(object_id, ...)';
              END;
            $$;
            COMMENT ON FUNCTION {schema}.search_{fieldname}(Integer) IS
            $$This function has been abandoned. Use {schema}.search_{fieldname}(object_id) instead.$$;

        """).lstrip().format(**locals()))

    def print_alias(self, schema, alias, fieldname):
        self.fileobj.write(textwrap.dedent("""
            CREATE OR REPLACE FUNCTION
              {schema}.search_{alias}
              ( IN   object_id   Bigint
              , OUT  is_in_field Boolean
              )
            LANGUAGE SQL
            IMMUTABLE
            AS $$
              SELECT {schema}.search_{fieldname}(object_id);
            $$;
            COMMENT ON FUNCTION {schema}.search_{alias}(Bigint) IS
            $$Alias of {schema}.search_{fieldname}(object_id).$$;

        """).lstrip().format(**locals()))

        # Warn about abandoned API
        self.fileobj.write(textwrap.dedent("""
            CREATE OR REPLACE FUNCTION
              {schema}.search_{alias}
              ( IN   skymap_id   Integer
              , OUT  is_in_field Boolean
              )
            LANGUAGE PLPGSQL
            IMMUTABLE
            AS $$
              BEGIN
                  RAISE EXCEPTION 'search_{alias}(skymap_id, ...) has been replaced by search_{alias}(object_id, ...)';
              END;
            $$;
            COMMENT ON FUNCTION {schema}.search_{alias}(Integer) IS
            $$This function has been abandoned. Use {schema}.search_{alias}(object_id) instead.$$;

        """).lstrip().format(**locals()))


    def warning(self, message):
        self.fileobj.write(textwrap.dedent("""
            ----------------------------------------
            -- Warning: {message}
            ----------------------------------------
        """).lstrip().format(**locals()))


@Printer.register("json")
class JSONPrinter(object):
    def __init__(self, fileobj):
        self.fileobj = fileobj
        self.separator = ""

    def __enter__(self):
        self.separator = "\n"
        self.fileobj.write('{')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.separator != "\n":
            # One or more records have been printed,
            # but the last record does not have terminating "\n" yet.
            self.fileobj.write('\n')
        self.fileobj.write('}\n')

    def print_group(self, schema, fieldname, tracts):
        """
        Print a group of tracts as a JSON object.
        @param schema (str)
        @param fieldname (str)
        @param tracts (list of integers)
        """
        tracts = sorted(tracts)

        self._print_separator()
        self.fileobj.write('"{schema}.{fieldname}": '.format(**locals()))
        punct = "["
        for i, tract in enumerate(tracts):
            self.fileobj.write(punct); punct = ","
            self.fileobj.write("\n  " if i % 10 == 0 else " ")
            self.fileobj.write(str(tract))

        if punct == "[":
            self.fileobj.write("[]")
        else:
            self.fileobj.write("\n]")

    def print_alias(self, schema, alias, fieldname):
        self._print_separator()
        self.fileobj.write(textwrap.dedent("""
            "{schema}.{alias}": {{"alias": "{schema}.{fieldname}"}}
        """).strip().format(**locals()))

    def warning(self, message):
        self._print_separator()
        self.fileobj.write(textwrap.dedent("""
            ////////////////////////////////////////
            // Warning: {message}
            ////////////////////////////////////////
        """).lstrip().format(**locals()))

        self.separator = "" # don't print "," next time

    def _print_separator(self):
        self.fileobj.write(self.separator)
        self.separator = ",\n"

def list_to_spans(integers):
    """
    Transform list of integers to list of spans.
    e.g.
        [1, 2, 3, 6, 7, 9]
        => [[1,3], [6,7], [9,9]]

    Both ends of spans are *inclusive* ;
    or a span [a,b] is equivalent to range(a, b+1)
    """

    integers = sorted(integers)
    length   = len(integers)
    spans = []

    first = 0
    while first < length:
        last = first
        while last+1 < length and integers[last]+1 == integers[last+1]:
            last += 1

        spans.append((integers[first], integers[last]))
        first = last+1

    return spans


if __name__ == "__main__":
    startup()
