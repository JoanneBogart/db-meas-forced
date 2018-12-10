# Copyright notice??

class DbddView(object):
    """
    Tool for making DPDD quantities, derived from native quantities
    to be found in tables in supplied list, available in a view.
    This class can generate the string used to create the view.

    dbschema               Name of schema in Postgres db
    tables                 List of tablenames to be joined such that
                           all inputs needed for DPDD quantities are
                           contained in them.  
                           position table should be first
    transforms             List of tuples consisting of 
                             * list of input native strings.  May include
                               variables ERR, FLUX, BAND
                             * form for output string name.  May include BAND
                               variable
                             * function for computing value. If None, single
                               input string is renamed to output string
    bands                  List of strings:
    dm_schema_version      Determines whether old or new identifiers
                           for err and flux are in native quantities

    """
    def __init__(self, dbschema, tables, transforms, bands, dm_schema_version):
        self.dbschema = dbschema
        self.tables = tables
        self.bands = bands
        if dm_schema_version == 1:
            self.ERR = 'sigma'
            self.FLUX = 'flux'
        else:
            self.ERR = 'err'
            self.FLUX = 'instflux'

    def view_string(self):
        dbschema = self.dbschema
        if len(tables) == 1:
            table_spec = '"{}"'.format(self.dbschema, tables[0])
        else:
            join_list = 
            [ '"{}"."{}"'.format(schemaName, tables[0]) ] + 
            [ 'LEFT JOIN "{}"."{}" USING (object_id)'.format(dbschema, table)
                 for table in tables[1:]
             ]
             table_spec = """
             """.join(join_list)
        cv = "create view {dbschema}.dpdd AS ( ".format(**locals())

        # Make a list of field definitions
