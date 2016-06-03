import json
import os
from datetime import date
from .exceptions import ValidationError
from .config import BASE_DB_FILE_PATH

class Database(object):
    def __init__(self, db_name, connect = False):
        self.name = db_name
        self.table_names = []
        self.path = BASE_DB_FILE_PATH + db_name + '/'
        if connect:
            self.read()
        else:
            if not os.path.exists(self.path):
                os.makedirs(self.path)

    def create_table(self, table_name=None, columns=[]):
        table = Table(self.name, table_name, columns)
        setattr(self, table_name, table)
        self.table_names.append(table_name)
    
    def show_tables(self):
        return self.table_names
        
    def read(self):
        files = os.listdir(self.path)
        for filename in files:
            with open(self.path + filename) as file:
                json_dict = json.load(file)
            table_name = filename
            self.table_names.append(table_name)
            table = Table(self.name, table_name, json_dict['config'], connect = True)
            setattr(self, table_name, table)
        

class Table(object):
    
    string_to_type = {'int' : int, 'str' : str, 'bool' : bool}
    
    def __init__(self, database_name, table_name, columns, connect = False):
        self.name = table_name
        self.path = BASE_DB_FILE_PATH + database_name + '/' + self.name
        
        #if os.path.exists(self.path):
            #self.read()
        #else:
        self.headers = [column['name'] for column in columns]
        self.types = [column['type'] for column in columns]
        self.data = [[] for _ in columns]
        self.config = columns
        
        if not connect:
            self.write()
        else:
            self.read()

    def insert(self, *args):
        if len(args) != len(self.data):
            raise ValidationError('Invalid amount of field')
        for value, column_list, column_type in zip(args, self.data, self.types):
            
            exec('type_ = ' + column_type, globals())
            if type(value) == type_:
                column_list.append(str(value))
            else:
                ind = args.index(value)
                type_string = str(type(value)).split(' ')[1].strip("'<>")
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(self.headers[ind], type_string, column_type))
        self.write()
    
    def read(self):
        with open(self.path, 'r') as file:
            json_dict = json.load(file)
            self.headers = json_dict['headers']
            self.types = json_dict['types']
            self.data = json_dict['data']
    
    def write(self):
        self.file = open(self.path, 'w')
        
        data = { "headers":self.headers, "types":self.types, "data":self.data, 'config': self.config}
        json_string = json.dumps(data)
        self.file.write(json_string)
        
        self.file.close()
        
    def count(self):
        return len(self.data[0])
        
    def query(self, **kwargs):

        #Only works for max of 1 kwarg        
        key, value = list(kwargs.items())[0] 
        if key in self.headers:
            column_number = self.headers.index(key)
            indices = [count for count, col_value in enumerate(self.data[column_number]) if value == col_value]
            values = [[data_list[ind] for data_list in self.data] for ind in indices]
            return_list = [Column(**dict(zip(self.headers, lst))) for lst in values]
    
        return return_list
    
    def describe(self):
        return self.config
        
    def all(self):
        indices = range(len(self.data[0]))

        values = [[convert_string(data_list[ind]) for data_list in self.data] for ind in indices]
        
        for lst in values:
            yield Column(**dict(zip(self.headers, lst)))
            
    def sort(self, attr_name, key_func = None):
        """return sorted list of columns"""
        get_attribute = lambda obj: getattr(obj, attr_name) # key function needs one argument
        
        if key_func:
            new_key = lambda obj: key_func(get_attribute(obj))
            return sorted(self.all(), key = new_key)
            
        return sorted(self.all(), key = get_attribute)

def convert_string(string):
    if string.isdigit():
        if '.' in string:
            return float(string)
        return int(string)
            
    if string == "True":
        return True
    elif string == "False":
        return False
    return string
        
class Column:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
            
def create_database(db_name):
    if os.path.exists( BASE_DB_FILE_PATH + db_name):
        raise ValidationError('Database with name "{}" already exists.'.format(db_name))
    return Database(db_name)
    
def connect_database(db_name):
    return Database(db_name, connect = True)
    