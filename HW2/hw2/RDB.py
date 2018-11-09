import pymysql
import json

cnx = pymysql.connect(host='localhost',
                              user='dbuser',
                              password='dbuser',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    print(q)
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result



def find_by_template(table, template, fields=None):
    q = "SELECT "
    if fields == None:
        q += "* "
    else:
        for field in fields:
            q += field + ","
        q = q[:-1]
    q += " FROM " + table + " WHERE "
    for k, v in template.items():
        q += k + " = "+ "'" + v + "'" + " AND "
    q = q[:-5]
    q += ";"

    result = run_q(q, None, True)
    return result



def insert(table,r):
    '''
    Insert a new row into the table.
    :param r: New row.
    :return: None. Table state is updated.
    '''

    sql = "INSERT INTO " + table + " ("
    for k,v in r.items():
        sql = sql + k + ","
    sql = sql[:-1]
    sql += ") VALUES (" 
    for k,v in r.items():
        sql = sql + "'" + v + "'" + ","
    sql = sql[:-1]
    sql += ");"

    result = run_q(sql, None, True)
    return result



def delete(table, t):
    '''
    Delete all tuples matching the template.
    :param t: Template
    :return: None. Table is updated.
    '''
            
    sql = "DELETE FROM " + table + " WHERE "
    for k, v in t.items():
        sql += k + " = " + "'" + v + "'" + " AND "
    sql = sql[:-5]
    sql += ";"

    result = run_q(sql, None, True)
    return result



def find_primary_key(table):
    sql = "SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name=" + "'" + table\
            + "'" + "AND CONSTRAINT_SCHEMA='HW2' AND constraint_name='PRIMARY';" 
    result = run_q(sql, None, True)
    return result



def find_foreign_key(table):
    sql = "SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME FROM \
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '" + table + "';"
    result = run_q(sql, None, True)
    return result

 


def teammates(playerid):
    sql = "with team as (select distinct teamID from Appearances where playerID='" + playerid + "')," +\
	    "years as (select distinct yearID from Appearances where playerID='" + playerid + "')," +\
        "teammates(tplayerID,tyearID) as (select playerID,yearID from Appearances " +  \
        "where teamID in (select * from team) and yearID in (select * from years)) " + \
	    "select '" + playerid + "', tplayerID playerID,min(tyearID) startyear,max(tyearID) endyear,count(tyearID) seasons from teammates " +\
		"group by tplayerID order by tplayerID;"
    print(sql)

    result = run_q(sql, None, True)
    return result



def career_stats(playerid):
    sql = "CREATE OR REPLACE VIEW career_stats AS " + \
	    "select batting.playerID, batting.teamID, batting.yearID, G_all, H as hits, AB as ABs, A, E as Errors " + \
        "from batting join fielding on batting.playerid = fielding.playerid and batting.yearID = fielding.yearID and batting.teamID = fielding.teamID " + \
        "join appearances on appearances.playerid = batting.playerid and appearances.yearID = batting.yearID and appearances.teamID = batting.teamID;"
    run_q(sql, None, True)

    q = "select playerID, teamID, yearID, G_all, hits, sum(A) as Assists, Errors from career_stats where playerID = '" + playerid + "' group by playerID, teamID, yearID, G_all, hits, Errors;"
    result = run_q(q, None, True)
    return result



def roster(teamid,yearid):
    sql = "CREATE OR REPLACE VIEW roster AS " + \
	    "select nameLast, nameFirst, people.playerID, appearances.teamID, appearances.yearID, G_all, H as hits, AB as abs, A, E " + \
        "from people join appearances on people.playerid = appearances.playerid " + \
        "join batting on people.playerID = batting.playerID and batting.yearID = appearances.yearID and appearances.teamID = batting.teamID " + \
	    "join fielding on people.playerID = fielding.playerID and batting.yearID = fielding.yearID and batting.teamID = fielding.teamID " + \
        "order by playerID;"
    run_q(sql,None,True)

    q = "select nameLast, nameFirst, playerID, teamID, yearID, G_all, hits, abs, sum(A) as attempts, sum(E) as errors from roster where teamid='" + teamid + "' and yearID = '" + yearid + "' " + \
	    "group by nameLast, nameFirst, playerID, teamID, yearID, G_all, hits, abs;"
    result = run_q(q, None, True)
    return result