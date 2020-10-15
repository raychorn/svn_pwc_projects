import six

def parse_sql_statement(statement):
    import sqlparse
    def parse_sql_columns(sql):
        columns = []
        parsed = sqlparse.parse(sql)
        stmt = parsed[0]
        for token in stmt.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(identifier.get_real_name())
            if isinstance(token, sqlparse.sql.Identifier):
                columns.append(token.get_real_name())
            if (token.ttype is not None) and (token.ttype.is_keyword):  # from
                continue
        return columns         
    return parse_sql_columns(statement)


class SQLCoder(object):
    def __init__(self, query_dict, ecf_data={}):
        self.query_dict = query_dict
        self.ecf_data = ecf_data
        self.base_sql = None  # everything but the where clause for later use during SQL fan-out.
        
    def normalize_sql_date(self, date_val):
        from misc import normalize_date_string
        return "TO_DATE('%s','MM/DD/YYYY')" % (normalize_date_string(date_val))
        
    def make_sql_statement(self, only_base_sql=True):
        from misc import normalize_date_string
        from misc import is_mmddyyyy_or_yyyymmdd
    
        def format_values_for(values, data_type=None):
            import collections
            is_iterable = isinstance(values, collections.Iterable) and (not isinstance(values, six.string_types))
            __values__ = [values] if (not is_iterable) else values
            is_quotable = (str(data_type).lower() in ['text'])
            __quote__ = "'" if (is_quotable) else ''
            __values__ = ['{}{}{}'.format(__quote__, val, __quote__) if (not is_mmddyyyy_or_yyyymmdd(val)) else self.normalize_sql_date(val) for val in __values__]
            return __values__ if (is_iterable) else __values__[0]
        
        def parameter_to_sql(parameter, table_name='', schema_name='', fields=[], ecf_data={}):
            __operator__ = parameter.get('Operation', '').lower()
            __values__ = parameter.get('Values', [])
            __type__ = parameter.get('Type', None)
            __thename__ = parameter.get('Name', '')
            __datasource__ = ecf_data.ecfjson.get('DataSource', {})
            if ( (__operator__ == 'BETWEEN'.lower()) and (len(__values__) == 2) ):
                return '{} {} {} AND {}'.format(schema_name+'.'+table_name+'.'+__thename__, __operator__.upper(), format_values_for(__values__[0], data_type=__type__), format_values_for(__values__[1], data_type=__type__))
            elif (__operator__ == 'IN'.lower()):
                resp = '{}.{} IN {}'.format(schema_name+'.'+table_name, __thename__, '(' + ', '.join(format_values_for(__values__, data_type=__type__)) + ')')
                return resp
            elif (len(__values__) == 1):
                return format_values_for(__values__[0], data_type=__type__)
            else:
                return ''
            
        def parameters_to_sql(parameters, table_name='', schema_name='', fields=[], ecf_data={}):
            items = [parameter_to_sql(parameter, table_name=table_name, schema_name=schema_name, fields=fields, ecf_data=ecf_data) for parameter in parameters]
            resp = ''
            if (len(items) > 0):
                resp = 'WHERE ' + '(' + ') AND ('.join(items) + ')'
            return resp
    
        def to_sql(d, ecf_data={}, only_base_sql=only_base_sql):
            table_field_list = []
            
            schema_name = d.get('Schema', '')
            table_name = d.get('Name', '')
            query_parameters = d.get('Parameters', [])
            fields = d.get('Columns', [])
    
            for field in fields:
                table_field_list.append(str(schema_name)+'.'+str(table_name)+'.'+str(field))
            parameters_sql = parameters_to_sql(query_parameters, table_name=table_name, schema_name=schema_name, fields=fields, ecf_data=ecf_data)
            self.base_sql = "{} {} {}".format('SELECT', ', '.join(table_field_list), 'FROM ' + str(schema_name)+'.'+str(table_name))
            return "{} {}".format(self.base_sql, parameters_sql) if (not only_base_sql) else self.base_sql
        
        __sql__ = to_sql(self.query_dict, ecf_data=self.ecf_data)
        return __sql__
