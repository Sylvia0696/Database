UNI: zr2209 
Name: Zhuangyu Ren

In the files named CSVTable.py and RDBtable.py, I implemented the required classes and functions in them.
And in RDBTable, there is no need for load() and save() by operating through MYSQL.


There are some constrictions for all the functions: 
1, For the load() functions in CSVTable and _init_() in RDBtable, the program will check whether the key columns are corresponded to existing columns or not, and whether the file exists in CSVTable or connection can be established in RDBtable
2, For the find_by_template/find_by_primary functions in both files, the program will check whether
the key entered(template) and the fields entered are consistent with the database columns
3, For the insert function, I checked the valid template keys and whether inserting is valid
4, For the delete function, the program will also check whether the keys are valid or not
5, For the save() functions in CSVTable, it will check whether the file can be written or not


I used Jupyter notebook to run the test cases, the test functions are defined in 2 test programs, and they can be tested in the functions below:
1, test_templates()
2, test_inserts()
3, test_deletes()
4, test_key()
My test processes were saved in two PDF files named RDB_test_screen and CSV_test_screen


There also exists two python programs separately imported CSVTable and RDBTable to find out the top ten hitters, files named RDB_top_hitter and CSV_top_hitter.