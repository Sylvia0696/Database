import pymysql
import json

cnx = pymysql.connect(host = "localhost",
                            user = "root",
                            password = "rzy19680722",
                            db = "HW1",
                            charset = 'utf8mb4',
                            cursorclass = pymysql.cursors.DictCursor)


cursor = cnx.cursor()
sql = "SELECT \
        Batting.playerID, \
        (SELECT People.nameFirst FROM People WHERE People.playerID=Batting.playerID) as first_name, \
        (SELECT People.nameLast FROM People WHERE People.playerID=Batting.playerID) as last_name, \
        sum(Batting.h)/sum(batting.ab) as career_average, \
        sum(Batting.h) as career_hits, \
        sum(Batting.ab) as career_at_bats,\
        min(Batting.yearID) as first_year, \
        max(Batting.yearID) as last_year \
        FROM \
        Batting \
        GROUP BY \
        playerId \
        HAVING \
        career_at_bats > 200 AND last_year >= 1960 \
        ORDER BY \
        career_average DESC \
        LIMIT 10;"
cursor.execute(sql)
re = cursor.fetchall()
print(json.dumps(re, indent=2))