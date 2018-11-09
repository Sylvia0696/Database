# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request #request return args(dict) contain offset and limit
import copy

import RDB

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def parse_and_print_args():

    fields = None
    in_args = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args)) #template
        fields = copy.copy(in_args.get('fields', None)) #fields
        offset = copy.copy(in_args.get('offset', None)) #offset
        limit = copy.copy(in_args.get('limit', None)) #limit
        if fields:
            del(in_args['fields'])
        if offset:
            del(in_args['offset'])
        if limit:
            del(in_args['limit'])

    try:
        if request.data:
            body = json.loads(request.data)
            print("body:",body)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None

    print("Request.args : ", json.dumps(in_args))
    return in_args, fields, body, offset, limit






@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource(resource):

    in_args, fields, body, offset, limit = parse_and_print_args()
    for k,v in in_args.items():
        in_args[k] = v[0]
    
    # print("in_ags is :",in_args)
    # print("fields is",fields)
    if request.method == 'GET':
        result = RDB.find_by_template(resource, \
                                           in_args, fields)
        strurl = request.url
              
        if len(result) > 10:
            if offset and limit:
                result1 = result[offset:limit]
                offset += 10  
                strurl1 = strurl + "&offset=" + offset + "&limit=10"        
            else:
                strurl1 = strurl + "&offset=10&limit=10"
                result1 = result[0:10]

            url = []
            url.append({"current":strurl})
            url.append({"next":strurl1})
            result1.append(url)

            return json.dumps(result1), 200, \
                        {"content-type": "application/json; charset: utf-8"}
                        
                        
        else:
            return json.dumps(result), 200, \
                        {"content-type": "application/json; charset: utf-8"}

    elif request.method == 'POST':
        result = RDB.insert(resource, \
                                    in_args)    
        result1 = RDB.find_by_template(resource, \
                                            in_args)
        return json.dumps(result1), 200, \
                    {"content-type": "application/json; charset: utf-8"}
    




@app.route('/api/<resource>/<primary_key>', methods=['GET', 'PUT','DELETE'])
def get_resource_key(resource,primary_key):

    in_args, fields, body, offset, limit = parse_and_print_args()
    for k,v in in_args.items():
        in_args[k] = v[0]
    
    key_column = RDB.find_primary_key(resource)
    column = []
    for pair in key_column:
        column.append(pair['column_name'])
    primary_key = primary_key.split('_')
    
    temp = {}
    for i in range(len(column)):
        temp[column[i]] = primary_key[i]
    print(temp)

    if request.method == 'GET':
        result = RDB.find_by_template(resource, \
                                           temp, fields)
        return json.dumps(result), 200, \
                    {"content-type": "application/json; charset: utf-8"}

    elif request.method == 'PUT':
        result = RDB.delete(resource, \
                                    temp)    
        result2 = RDB.insert(resource, \
                                    in_args)
        result3 = RDB.find_by_template(resource, \
                                            in_args)
        return json.dumps(result3), 200, \
                    {"content-type": "application/json; charset: utf-8"}

    elif request.method == 'DELETE':
        result = RDB.delete(resource,temp)
        return json.dumps(result), 200, \
                    {"content-type": "application/json; charset: utf-8"}

    




@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', 'POST'])
def dependent_get_resource(resource,primary_key,related_resource):
    in_args, fields, body, offset, limit = parse_and_print_args()
    for k,v in in_args.items():
        in_args[k] = v[0]
    
    columns = RDB.find_foreign_key(resource)
    related_col = []
    print("columns",columns)
    for i in columns:
        if i['REFERENCED_TABLE_NAME'] == related_resource:
            related_col.append(i['REFERENCED_COLUMN_NAME'])
    print("related_cols",related_col)

    
    key_column = RDB.find_primary_key(resource)
    column = []
    for pair in key_column:
        column.append(pair['column_name'])
    primary_key = primary_key.split('_')
    
    temp = {}
    for i in range(len(column)):
        temp[column[i]] = primary_key[i]
    print(temp)


    related_value = RDB.find_by_template(resource, \
                                        temp, related_col)
    related_value = related_value[0]

    print("related_value",related_value)
    
    if  request.method == 'GET':
        result = RDB.find_by_template(related_resource, \
                                            related_value, fields)
        return json.dumps(result), 200, \
                        {"content-type": "application/json; charset: utf-8"}
    
    elif request.method == 'POST':
        RDB.delete(related_resource, related_value)
        result = RDB.insert(related_resource, related_value)
        result1 = RDB.find_by_template(related_resource, related_value)
        return json.dumps(result1), 200, \
                        {"content-type": "application/json; charset: utf-8"}
        

    




@app.route('/api/people/<playerid>/career_stats', methods=['GET'])
def get_career_stats(playerid):
    result = RDB.career_stats(playerid)
    strurl = request.url
              
    in_args, fields, body, offset, limit = parse_and_print_args()
    if len(result) > 10:
        if offset and limit:
            result1 = result[offset:limit]
            offset += 10  
            strurl1 = strurl + "&offset=" + offset + "&limit=10"        
        else:
            strurl1 = strurl + "&offset=10&limit=10"
            result1 = result[0:10]

        url = []
        url.append({"current":strurl})
        url.append({"next":strurl1})
        result1.append(url)

        return json.dumps(result1), 200, \
                    {"content-type": "application/json; charset: utf-8"}
                    
                    
    else:
        return json.dumps(result), 200, \
                    {"content-type": "application/json; charset: utf-8"}







@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_teammates_id(playerid):
    result = RDB.teammates(playerid)
    strurl = request.url
              
    in_args, fields, body, offset, limit = parse_and_print_args()
    if len(result) > 10:
        if offset and limit:
            result1 = result[offset:limit]
            offset += 10  
            strurl1 = strurl + "&offset=" + offset + "&limit=10"        
        else:
            strurl1 = strurl + "&offset=10&limit=10"
            result1 = result[0:10]

        url = []
        url.append({"current":strurl})
        url.append({"next":strurl1})
        result1.append(url)

        return json.dumps(result1), 200, \
                    {"content-type": "application/json; charset: utf-8"}
                    
                    
    else:
        return json.dumps(result), 200, \
                    {"content-type": "application/json; charset: utf-8"}






@app.route('/api/roster?teamid=<teamid>&yearid=<yearid>', methods=['GET'])
def roster(teamid,yearid):
    result = RDB.roster(teamid,yearid)
    strurl = request.url
              
    in_args, fields, body, offset, limit = parse_and_print_args()
    if len(result) > 10:
        if offset and limit:
            result1 = result[offset:limit]
            offset += 10  
            strurl1 = strurl + "&offset=" + offset + "&limit=10"        
        else:
            strurl1 = strurl + "&offset=10&limit=10"
            result1 = result[0:10]

        url = []
        url.append({"current":strurl})
        url.append({"next":strurl1})
        result1.append(url)

        return json.dumps(result1), 200, \
                    {"content-type": "application/json; charset: utf-8"}
                    
                    
    else:
        return json.dumps(result), 200, \
                    {"content-type": "application/json; charset: utf-8"}






if __name__ == '__main__':
    app.run()