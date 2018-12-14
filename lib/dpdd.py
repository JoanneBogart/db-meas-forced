# Copyright notice??
from yaml import load as yload
import re

class DpddYaml(object):
    """
    Parse and validate Yaml file
      - must be a list
      - each list entry must be a dict
      - each dict must have key DPDDname
    Return dict of dicts, keyed by DPDDname
    """
    def __init__(self, infile):
        self.inf = infile        # string or  file pointer to yaml text file

    def parse(self):
        y = yload(self.inf)
        if type(y) != type([]):
            raise TypeError("Input is not a list")
        dpdd_dict = {}
        for i in y:
            if type(i) != type({}):
                raise TypeError("Item is not a dict")
            if 'DPDDname' not in i:
                raise AttributeError("Entry missing DPDDname")
            if 'RPN' not in i and len(i['NativeInputs']) != 1:
                raise AttributeError("Missing RPN attribute")
        return y


class DpddView(object):
    """
    Tool for making DPDD quantities, derived from native quantities
    to be found in tables in supplied list, available in a view.
    This class can generate the string used to create the view.

    dbschema               Name of schema in Postgres db. Required
    tables                 List of tablenames to be joined such that
                           all inputs needed for DPDD quantities are
                           contained in them.  
                           position table should be first
    yaml_path              Path to yaml file describing transformations
    override_path          Path to yaml file describing Postgres-specific
                           overrides

    bands                  List of strings:
    dm_schema_version      Determines whether old or new identifiers
                           for err and flux are in native quantities
                           
                           Allowable values are 1,2 or 3
    """
    def __init__(self, dbschema, tables=['position', 'dpdd_ref', 'dpdd_forced'],
                 bands=['g','i','r','u','y','z'], 
                 yaml_path='native_to_dpdd.yaml', pixel_scale=0.2,
                 yaml_override=None, dm_schema_version=3):
        self.dbschema = dbschema
        self.tables = tables
        self.yaml_path = yaml_path
        self.yaml_override = yaml_override
        self.dm_schema_version = dm_schema_version
        self.bands = bands
        self.pixel_scale=pixel_scale
        self.ERR = 'err'
        self.FLUX = 'instflux'
        if dm_schema_version not in (1,2,3):
            raise ValueError('Unsupported schema version {}'.format(str(db_schema_version)))

        if dm_schema_version == 1: self.ERR = 'sigma'
        if dm_schema_version < 3: self.FLUX = 'flux'

    # Make a list of field definitions 
    def name_dict(lst):
        n_dict = {}
        for i in lst:
            itemdict = {}
            for k in ['NativeInputs', 'RPN']:
                if k in i:
                    itemdict[k] = i[k]
            if 'Datatype' in i:
                itemdict['Datatype'] = i['Datatype']
            else: itemdict['Datatype'] = 'float'
            n_dict[i['DPDDname']] = itemdict
        return n_dict      

    @staticmethod
    def rpn_value(inputs, rpn):
        argstack = []
        varpat = re.compile('x(\d+)$')
        subspat = re.compile(r'\{[a-zA-Z_]*\}$')
        funcpat = re.compile('([a-zA-Z_]+[:a-zA-Z0-9_.]*)\(\)$')
        func2pat = re.compile('([a-zA-Z_]+[:a-zA-Z0-9_.]*)\(,\)$')
        for elt in rpn:
            #print("found elt {} of type {}".format(elt, type(elt)))
            try:
                s = float(elt)
                argstack.append(str(elt))
                continue
            except ValueError:
                pass
            m = varpat.match(str(elt))
            if m:
                i = int(m.group(1))
                if i > len(rpn):
                    raise ValueError('RPN elt {} references non-existent input'.format(elt))
                # push onto stack
                argstack.append(inputs[i - 1])
                continue
            m = subspat.match(str(elt))
            if m:
                argstack.append(elt)
                continue
            if str(elt) in ['*', '+', '-', '/', '^', '|', '%', '&', 'or', 'and']:
                res = '({} {} {})'.format(argstack.pop(),elt,argstack.pop())
                argstack.append(res)
                continue
            if str(elt) in ['!', 'not']:
                res = '({} {})'.format(elt, argstack.pop())
                argstack.append(res)
                continue
            m = funcpat.match(str(elt))
            if m:
                f = m.group(1)
                if ':' in f:
                    f = '"' + f + '"'
                res = '{}({})'.format(f, argstack.pop())
                argstack.append(res)
                continue
            m = func2pat.match(str(elt))
            if m:
                f = m.group(1)
                if ':' in f:
                    f = '"' + f + '"'
                res = '{}({},{})'.format(f, argstack.pop(), 
                                         argstack.pop())
                argstack.append(res)
                continue
            else:
                raise ValueError('Unknown element {} in RPN list'.format(elt))
        if (len(argstack) != 1 ):
            raise ValueError('Bad RPN list')
        return argstack.pop()    

    def resolve(self, item_dict):
        # most common case is single native mapping to dpdd field
        # Return a list because we need it if BAND appears

        if 'RPN' in item_dict:
            value = self.rpn_value(item_dict['NativeInputs'], 
                                   item_dict['RPN'])
        else:
            value = item_dict['NativeInputs'][0]

        # Make AS string
        asv = '{} AS {}'.format(value, item_dict['DPDDname'])
        FLUX = self.FLUX
        ERR = self.ERR
        PIXEL_SCALE = self.pixel_scale
        BAND = '{BAND}'
        asv = asv.format(**locals())
        asvl = []
        if re.match(r".*\{BAND\}.*", asv):
            for b in self.bands:
                f = re.sub(r"\{BAND\}",b, asv)
                #f = asv.format(b, b)
                asvl += [f]
            return asvl
        else: return [asv]        
        
    def view_string(self):
        dbschema = self.dbschema
        if len(self.tables) == 1:
            table_spec = '"{}"."{}"'.format(dbschema, self.tables[0])
        else:
            join_list = [ '"{}"."{}"'.format(dbschema, self.tables[0]) ] 
            for table in self.tables[1:]:
                join_list.append('LEFT JOIN "{}"."{}" USING (object_id)'.format(dbschema, table) )
                  
            table_spec = """
            """.join(join_list)
        
        dpdd_yaml = DpddYaml(open(self.yaml_path)).parse()
        if self.yaml_override:
            override_yaml = DpddYaml(open(self.yaml_override)).parse()
            for i in override_yaml:
                # Find elt in dpdd_yaml with same DPDDname
                # delete
                # add override entry instead.
                for j in dpdd_yaml:
                    if j['DPDDname'] == i['DPDDname']:
                        j['NativeInputs'] = i['NativeInputs']
                        for key in ['Datatype', 'RPN']:
                            if key in i: j[key] = i[key]

        fields = []
        for i in dpdd_yaml:
            r = self.resolve(i)
            if r: fields += r
            #r = DpddYaml.resolve(i)
            #if r: fields.append(r)
        sFields = """,
        """.join(fields)

        cv = """CREATE VIEW {dbschema}.dpdd AS ( 
             SELECT
                   {sFields}
             FROM
                   {table_spec}
           )
        """.format(**locals())
        return cv

import sys
if __name__ =='__main__':
    if len(sys.argv) > 1: 
        yaml_file = sys.argv[1] 
    else: yaml_file = 'native_to_dpdd.yaml'
    if len(sys.argv) > 2: 
        override_file = sys.argv[2] 
    else: override_file = None

    view = DpddView('run12p_native', yaml_path = yaml_file, 
                    yaml_override = override_file,
                    dm_schema_version = 1)

    cv = view.view_string()

    print(cv)

    res = DpddView.rpn_value(['the_first', 'the_second'],
                             ['x1', 'x2', '+','x1','*'])

    print(res)
