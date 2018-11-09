import csv          # Python package for reading and writing CSV files.
import copy         # Copy data structures.
import json

import sys,os

# You can change to wherever you want to place your CSV files.
rel_path = os.path.realpath('./Data')

class CSVTable():

    # Change to wherever you want to save the CSV files.
    data_dir = rel_path + "/"

    def __init__(self, table_name, table_file, key_columns):
        '''
        Constructor
        :param table_name: Logical names for the data table.
        :param table_file: File name of CSV file to read/write.
        :param key_columns: List of column names the form the primary key.
        '''
        self.path = rel_path + "/" + table_file
        self.name = table_name
        self.columns = key_columns
        self.data = []
#         print(self.path)
                
        
    
    
    def load(self):
        '''
        Load information from CSV file.
        :return: None
        '''
        try:
            with open(self.path, 'r+') as f:
                f = csv.DictReader(f) 
                for r in f:
                    self.data.append(r)
        except IOError:
            print("This file does not exist")
            
        
        for col in self.columns:
            if col not in self.data[0]:
                raise ValueError("No such key_column in columns")
                
                


    def __str__(self):
         return json.dumps(self.data, indent=2)



        
                   
                
    def find_by_template(self, t, fields = None):
        '''
        Return a table containing the rows matching the template and field selector.
        :param t: Template that the rows much match.
        :param fields: A list of columns to include in responses.
        :return: CSVTable containing the answer.
        '''
        for key in t.keys():
            if key not in self.data[0]:
                raise ValueError("Key does not match")

        re = []

        for i in self.data:
            match = 0
            for k,v in t.items():
                if i[k] == v:
                    match += 1
            if match == len(t):
                if fields == None:
                    re.append(i)
                else:
                    tmp = {}
                    for field in fields:
                        tmp1 = {field:i[field]}
                        tmp.update(tmp1) 
                    re.append(tmp)
        print(re)
        return re




    def find_by_primary_key(self, string, fields = None):
        '''
        Input is a string of values.
        Fields is a list defining which of the fields from the row/tuple you want.
        Output is the single dictionary in the table that is the matching result, or null/None.
        '''
        if len(string) != len(self.columns):
            raise ValueError("Length of value does not match keys")

        r = []
        
        for i in self.data:
            count = 0
            for j in range(0, len(string)):
                if i[self.columns[j]] == string[j]:
                    count += 1
            if count == len(string):
                if fields == None:
                    r.append(i)
                else:
                    tmp = {}
                    for field in fields:
                        tmp1 = {field:i[field]}
                        tmp.update(tmp1) 
                    r.append(tmp)
        print(r)
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
            
        print(r)

        self.data.append(r)
        
        
        
        
    def delete(self, t):
        '''
        Delete all tuples matching the template.
        :param t: Template
        :return: None. Table is updated.
        '''
        for key in t.keys():
            if key not in self.data[0]:
                raise ValueError("Key does not match")
        
        # print("calling the function")
        for i in self.data:
            match = 0
            for key in t.keys():
                if i[key] == t[key]:
                    match += 1
            if match == len(t):
                self.data.remove(i)
                
                
                
                
    def save(self):
        '''
        Write updated CSV back to the original file location.
        :return: None
        '''
        try:
            with open(self.path, 'w',newline='') as f:
                columns = self.data[0].keys()
                file = csv.DictWriter(f,fieldnames = columns)
                file.writeheader()
                for r in self.data:
                    file.writerow(r) 
        except IOError:
            print("This file can not be saved")