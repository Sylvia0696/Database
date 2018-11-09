import pymysql
import json




class RDBTable:


    def __init__(self, table_name, key_columns, host="localhost", user="root", passwd="rzy19680722", db="HW1"):
        '''
        A constructor or initializer that takes the following parameters:
        A string that identifies the location to connect to the file or database table.
        A "name" for the created DataTable.
        The set of column names (dictionary keys) that form the primary key for elements of the table.
        '''
        self.cnx = pymysql.connect(host = host,
                                    user = user,
                                    password = passwd,
                                    db = db,
                                    charset = 'utf8mb4',
                                    cursorclass = pymysql.cursors.DictCursor)
        self.name = table_name
        self.columns = key_columns
        self.col = []
        self.cols = []
        self.cursor = self.cnx.cursor()
        
        

        sql = "SHOW COLUMNS FROM " + self.name + ";"
        self.cursor.execute(sql)
        self.col = self.cursor.fetchall()

    
        for i in self.col:
            self.cols.append(i["Field"])

        for column in self.columns:
            if column not in self.cols:
                raise ValueError("No such key_column in columns")





    def find_by_primary_key(self, string, fields = None):
        '''
        Input is a string of values.
        Fields is a list defining which of the fields from the row/tuple you want.
        Output is the single dictionary in the table that is the matching result, or null/None.
        '''
        if len(string) != len(self.columns):
            raise ValueError("Length of value does not match keys")

        self.cursor = self.cnx.cursor()
        sql = "SELECT "
        
        
        if fields == None:
            sql += " * "
        
        else:
            for field in fields:
                sql += field + ","
            sql = sql[:-1]
        sql += " FROM " + self.name + " WHERE "
        for index in range(len(string)):
            sql += self.columns[index] + " = " + "'" + string[index] + "'" + " AND "
        sql = sql[:-5]
        sql += ";"
        
#         print(sql)
        self.cursor.execute(sql)
        r = self.cursor.fetchall()
        print(r)
        return r
    
   


    
    def find_by_template(self, t, fields=None):
        '''
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        '''
        self.cursor = self.cnx.cursor()
        
        sql = "SELECT "
        if fields == None:
            sql += "* "
        else:
            for field in fields:
                sql += field + ","
            sql = sql[:-1]
        sql += " FROM " + self.name + " WHERE "
        for k, v in t.items():
            sql += k + " = "+ "'" + v + "'" + " AND "
        sql = sql[:-5]
        sql += ";"
       
        #print(sql)
        self.cursor.execute(sql)
        r = self.cursor.fetchall()
        return r
    
    
    
    
    def insert(self, r):
        '''
        Insert a new row into the table.
        :param r: New row.
        :return: None. Table state is updated.
        '''
        key_value = []
        for i in self.columns:
            key_value.append(r[i])
            
        if key_value == None:
            raise ValueError("No primary key")
        
        re = self.find_by_primary_key(key_value)
        if len(re) != 0:
            raise ValueError("This row exists")

        self.cursor = self.cnx.cursor()
        sql = "INSERT INTO " + self.name + " ("
        for k,v in r.items():
            sql = sql + k + ","
        sql = sql[:-1]
        sql += ") VALUES (" 
        for k,v in r.items():
            sql = sql + "'" + v + "'" + ","
        sql = sql[:-1]
        sql += ");"

        #print(sql)
        self.cursor.execute(sql)
        self.cnx.commit()
    
    
    
    
    def delete(self, t):
        '''
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        '''
        t_keys = t.keys()
        for i in t_keys:
            if i not in self.cols:
                raise ValueError("Delete Error")
                
        self.cursor = self.cnx.cursor()
        sql = "DELETE FROM " + self.name + " WHERE "
        for k, v in t.items():
            sql += k + " = " + "'" + v + "'" + " AND "
        sql = sql[:-5]
        sql += ";"
        
        #print(sql)
        self.cursor.execute(sql)
        self.cnx.commit()