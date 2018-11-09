from CSVTable import CSVTable
import json
import sys, os


def test1():

    csvt = CSVTable("People", "People.csv", ["playerID"])
    csvt.load()
    print("Table = ", csvt)




def test_primary_key(test_name, table_name, table_file, key_columns, string, fields=None):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("String = ", string)

    try:
        csvt = CSVTable(table_name, table_file, key_columns)
        csvt.load()
        r = csvt.find_by_primary_key(string, fields)
        print("Result table:")
        print(json.dumps(r,indent=2))
        
    except ValueError as ve:
        print("Exception = ", ve)




def test_template(test_name, table_name, table_file, key_columns, template, fields=None):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)
    print("Fields = ", fields)

    try:
        csvt = CSVTable(table_name, table_file, key_columns)
        csvt.load()
        r = csvt.find_by_template(template, fields)
        print("Result table:")
        print(json.dumps(r,indent=2))

    except ValueError as ve:
        print("Exception = ", ve)




def test_insert(test_name, table_name, table_file, key_columns, row):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Row to insert = ", row)

    try:
        csvt = CSVTable(table_name, table_file, key_columns)
        csvt.load()
        csvt.insert(row)
        csvt.save()

    except ValueError as ve:
        print("Exception = ", ve)




def test_delete(test_name, table_name, table_file, key_columns, template):
    print("\n\n*******************************")
    print("Test name = ", test_name)
    print("Template = ", template)

    try:
        csvt = CSVTable(table_name, table_file, key_columns)
        csvt.load()
        csvt.delete(template)
        csvt.save()
        print("the delete function finished")

    except ValueError as ve:
        print("Exception = ", ve)














#test1()


def test_templates():
    test_template("Test2", "People", "People.csv", ["playerID"],
                  {"birthMonth": "9", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test3", "People", "People.csv", ["playerID"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test4", "People", "People.csv", ["canary"],
                  {"nameFirst": "Ted", "nameLast": "Williams"}, ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_template("Test5", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01"}, ["playerID", "yearID", "teamID", "AB", "H", "HR"])

    test_template("Test6", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "iq": 100}, ["playerID", "yearID", "teamID", "AB", "H", "HR"])

    test_template("Test7", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "yearID": "1961"}, ["playerID", "yearID", "teamID", "AB", "H", "HR"])

    test_template("Test7", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"],
                  {"playerID": "willite01", "yearID": "1960"}, ["playerID", "yearID", "teamID", "AB", "H", "HR", "Age"])


def test_inserts():

    test_insert("Insert Test 1", "People", "People.csv", ["playerID"],
                {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"})

    test_primary_key("Find after insert 1", "People", "People.csv", ["playerID"],
                ["dff1"], ["nameLast", "nameFirst", "birthMonth", "birthYear"])

    try:
        test_insert("Insert Test 2", "People", "People.csv", ["playerID"],
                    {"playerID": "dff1", "nameLast": "Ferguson", "nameFirst": "Donald"})

        raise ValueError("That insert should not have worked!")

    except ValueError:
        print("OK. Did not insert duplicate key.")
        
        


def test_deletes():
    
    test_delete("Delete Test 1", "People", "People.csv", ["playerID"], 
                {"birthMonth": "9", "nameLast": "Williams"})

    
    test_template("Find after delete 1","People", "People.csv", ["playerID"], 
                {"birthMonth": "9", "nameLast": "Williams"})



    try:
        test_delete("Delete Test 1", "People", "People.csv", ["playerID"], 
                    {"birthMonth": "13", "nameLast": "Williams"})
        raise ValueError("No such record exist")
    
    except ValueError:
        print("OK, do not delete here")




def test_key():

    test_primary_key("Primary_key Test 1", "People", "People.csv", ["playerID"], ["willite01"], None)

    test_primary_key("Primary_key Test 2", "People", "People.csv", ["playerID"], 
                ["alvarcl01"],["nameLast", "nameFirst", "birthMonth", "birthYear"])

    test_primary_key("Primary_key Test 3", "Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"], 
                ["aardsda01","2015", "ATL", "1"],["playerID", "yearID", "teamID", "AB", "H", "HR"])