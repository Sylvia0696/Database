from CSVTable import CSVTable
import json
import sys, os





csvt_p = CSVTable("People", "People.csv", ["playerID"])
csvt_p.load()

csvt_b = CSVTable("Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"])
csvt_b.load()

player = set()
for i in csvt_b.data:
    if int(i["yearID"]) >= 1960:
        player.add(i["playerID"])

list_all = []

for i in player:
    total_bats = 0
    total_hits = 0
    
    for j in csvt_b.data:
        if j["playerID"] == i:
            total_bats += int(j["AB"])
            total_hits += int(j["H"])
    if total_bats > 200:
        batting_average = total_hits / total_bats
        list_one = {"player":i, "score":batting_average}
        list_all.append(list_one)

new_list = sorted(list_all,key = lambda e:e.__getitem__('score'),reverse = True)
new_list = new_list[0:11]

# import pdb
# pdb.set_trace()

re = []

for i in new_list:
    for j in csvt_p.data:
        if j["playerID"] == i["player"]:
            re_one = {"playerID":i["player"],"nameFirst":j["nameFirst"],"nameLast":j["nameLast"],"average":i["score"]}
            re.append(re_one)

print(json.dumps(re, indent=2))