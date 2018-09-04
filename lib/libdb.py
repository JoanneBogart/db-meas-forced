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

class DBConnectionDebug(object):
    """
    DB connection decorator for debugging
    """
    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def __enter__(self, *a, **b):
        return DBConnectionDebug(self.connection.__enter__(*a, **b))

    def __exit__(self, *a, **b):
        return self.connection.__exit__(*a, **b)

    def cursor(self, *a, **b):
        return DBCursorDebug(self.connection.cursor(*a, **b))

class DBCursorDebug(object):
    """
    DB cursor decorator for debugging.
      * execute() will print the command actually sent to the server.
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def __enter__(self, *a, **b):
        return DBCursorDebug(self.cursor.__enter__(*a, **b))

    def __exit__(self, *a, **b):
        return self.cursor.__exit__(*a, **b)

    def __iter__(self, *a, **b):
        return self.cursor.__iter__(*a, **b)

    def execute(self, *a, **b):
        sql = self.cursor.mogrify(*a, **b).strip().decode()
        print(sql if sql.endswith(";") else sql + ";")
        return self.cursor.execute(sql)
