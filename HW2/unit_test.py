import json
import RDB

def test1():
    t = {"nameLast": ["Williams"], "nameFirst": ["Ted"]}
    #print("WC = ", SimpleBO.template_to_where_clause(t))
    result = RDB.find_by_template("people", t, None)
    print("Result = ", json.dumps(result, indent=2))


test1()