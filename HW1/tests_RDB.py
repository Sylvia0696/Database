from RDBTable import RDBTable
import json
import sys, os


def test1():

    rdbt = RDBTable("People", ["playerID"])
    print("Table = ", rdbt)




def test_primary_key(test_name, table_name, string, key_columns, fields=None):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("String = ", string)

    try:
        rdbt = RDBTable(table_name, key_columns)
        r = rdbt.find_by_primary_key(string, fields)
        print("Result table:")
        print(json.dumps(r,indent=2))
        
    except ValueError as ve:
        print("Exception = ", ve)




def test_template(test_name, table_name, key_columns, template, fields=None):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)

    try:
        rdbt = RDBTable(table_name, key_columns)
        r = rdbt.find_by_template(template, fields)
        print("Result table:")
        print(json.dumps(r,indent=2))

    except ValueError as ve:
        print("Exception = ", ve)




def test_insert(test_name, table_name, key_columns, row):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Row to insert = ", row)

    try:
        rdbt = RDBTable(table_name, key_columns)
        rdbt.insert(row)

    except ValueError as ve:
        print("Exception = ", ve)




def test_delete(test_name, table_name, key_columns, template):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)

    try:
        rdbt = RDBTable(table_name, key_columns)
        rdbt.delete(template)

    except ValueError as ve:
        print("Exception = ", ve)









#test1()


def test_templates():
    test_template("Test2", "People", ["playerID"],
                  {"birthMonth": "9", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test3", "People", ["playerID"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test4", "People",  ["canary"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test5", "Batting", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01"}, ["playerID", "yearID", "teamID", "AB", "H", "HR"])

    

def test_inserts():

    test_insert("Insert Test 1", "People", ["playerID"],
                {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"})

    test_primary_key("Find after insert 1", "People", ["dff1"], ["playerID"],
                ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    try:
        test_insert("Insert Test 2", "People", ["playerID"],
                    {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"})

        raise ValueError("That insert should not have worked!")

    except ValueError:
        print("OK. Did not insert duplicate key.")


def test_deletes():
    
    test_delete("Delete Test 1", "PeopleSmall", ["playerID"], 
                {"playerID": "aardsda01"})

    
    test_primary_key("Find after delete 1","PeopleSmall", ["aardsda01"], ["playerID"])

    try:
        test_delete("Delete Test 1", "PeopleSmall", ["playerID"], 
                    {"birthMonth": "13"})
        raise ValueError("No such record exist")
    
    except ValueError:
        print("OK, do not delete here")




def test_key():

    test_primary_key("Primary_key Test 1", "People", ["willite01"], ["playerID"],None)

    test_primary_key("Primary_key Test 2", "People", ["alvarcl01"], ["playerID"], 
                ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_primary_key("Primary_key Test 3", "Batting", ["aardsda01","2015", "ATL", "1"],
                     ["playerID", "yearID", "teamID", "stint"],["playerID", "yearID", "teamID", "AB", "H", "HR"])


