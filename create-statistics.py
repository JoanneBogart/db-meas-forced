#!/usr/bin/env python3

import psycopg2

import contextlib
import getpass
import os


def startup():
    import argparse
    parser = argparse.ArgumentParser(description="""
        Create statistics for every "group" of columns,
        where a "group" means a set of the same measurements
        performed in different bands. e.g.
            (g_mag, r_mag, i_mag, z_mag, y_mag), and
            (g_magsigma, r_magsigma, i_magsigma, z_magsigma, y_magsigma).
    """)

    parser.add_argument('tables', metavar="table", nargs='+', help="""
        Table names
    """)

    parser.add_argument('--database', nargs='+', default=[], help="""
        Database descriptor. e.g. --database database=mydb user=admin
    """)
    parser.add_argument('--dry-run', action="store_true", help="""
        Do not change the database, but show SQL queries to be issued.
    """)

    args = parser.parse_args()
    args.database = dict(key_value.split('=', 1) for key_value in args.database)

    main(**vars(args))


def main(tables, database, dry_run=False):
    database = get_full_db_specifier(database)

    for table in tables:
        colgroups = get_colgroups(table, database)
        for group in colgroups:
            create_statistics(table, group, database)

        with contextlib.closing(psycopg2.connect(**database)) as db:
            cursor = db.cursor()
            cursor.execute(f"""
            ANALYZE {table}
            """)
            db.commit()


def get_full_db_specifier(database):
    """
    @param database (dict):
        Database specifier partially containing
            "database", "user", "password", "host", "port"
        If no password is required for the connection,
        set database["password"] = "trusted".
        If database["password"] is empty, the user is asked password.

    @return (dict) database specifier sufficient for connecting to it.
    """
    database = dict(database) if database else {}

    if (not database.get("database")
    and not database.get("dbname")
    ):
        database["database"] = getpass.getuser()

    if not database.get("user"):
        database["user"] = getpass.getuser()

    password = database.get("password", "")
    if password.lower() == "trusted":
        pass
    elif (not password) and (not os.environ.get("PGPASSWORD")):
        database["password"] = getpass.getpass(
            f"PostgreSQL password for {database['user']}: "
        )

    return database


def get_colgroups(table, database):
    """
    @param table (str): qualified-id for a table.
    @param database (dict): database specifier ({"dbname": ..., ...})
    @return
        [ [colname, colname,...], [colname, colname,...], ... ]
    """
    with contextlib.closing(psycopg2.connect(**database)) as db:
        cursor = db.cursor()
        cursor.execute("""
        SELECT attname FROM pg_attribute
        WHERE attrelid = %(table)s::Regclass::Oid
            AND attnum > 0
        """, locals()
        )

        columns = [c for c, in cursor.fetchall()]

    # split "filter_name" into ("filter", "name") and group'em by "name"
    groups = groupify((c.split("_", 1) for c in columns), key=lambda x: (x[1] if len(x) >= 2 else x[0]))

    # "groups" so far is  {name: [(filter, name), ...], ...}
    # but what we want is {name: {filter, ...}, ...}
    groups = {name: {x[0] for x in group} for name, group in groups.items()}

    # Names of some columns are not "filter_name",
    # and such names contaminate "groups".
    # We have to purge them.
    minimum_filter_set = {'g', 'r', 'i', 'z', 'y'}
    groups = {
        name: filters for name, filters in groups.items()
        if filters.issuperset(minimum_filter_set)
    }

    return [
        [filter + "_" + name for filter in filters]
        for name, filters in groups.items()
    ]


def create_statistics(table, columns, database):
    """
    @param table (str): qualified-id for a table.
    @param columns (list of str): column names
    @param database (dict): database specifier ({"dbname": ..., ...})
    """

    with contextlib.closing(psycopg2.connect(**database)) as db:
        cursor = db.cursor()

        params = []
        args = {}
        for i, c in enumerate(columns):
            params.append(f"%(col{i})s")
            args[f"col{i}"] = c

        params = ", ".join(params)
        args.update({"table": table})

        cursor.execute(f"""
        SELECT quote_ident(attname), attnum FROM pg_attribute
        WHERE attrelid = %(table)s::Regclass::Oid
            AND attname IN ({params})
        ORDER BY attnum ASC
        """, args
        )

        ret = cursor.fetchall()
        columns = [col for col, num in ret]
        num = ret[0][1]

        if table.endswith('"'):
            statname = f'{table[:-1]}_s{num}"'
        else:
            statname = f'{table}_s{num}'

        colseq = ', '.join(columns)

        cursor.execute(f"""
        CREATE STATISTICS IF NOT EXISTS {statname}
        ON {colseq}
        FROM {table}
        """
        )

        db.commit()


def groupify(iterable, key):
    """
    @param iterable (iterable)
    @param key (lambda elem: groupname)
    @return
        {groupname: [elem, ...], ...}
    """

    groups = {}

    for x in iterable:
        k = key(x)
        group = groups.get(k)
        if group is None:
            groups[k] = [x]
        else:
            group.append(x)

    return groups


if __name__ == "__main__":
    startup()
