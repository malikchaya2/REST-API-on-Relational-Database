# Lahman.py
# Convert to/from web native JSON and Python/RDB types.
import json
# Include Flask packages
from flask import Flask
from flask import request
import copy
import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def run_q(self, q, args, fetch=False):
    """
    :param q: The query string to run.
    :param fetch: True if this query produces a result and the function should perform and return fetchall()
    :return:
    """
    self.__debug_message__("run_q: q = " + q)

    cursor = self.cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
        return result
    self.cnx.commit()

def parse_and_print_args():

    fields = None
    in_args = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        print(" parse args not none ", request.args)
        fields = copy.copy(in_args.get('fields', None))
        if fields:
            del(in_args['fields'])

    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None
    print("Request.args : ", json.dumps(in_args))
    return in_args, fields, body


@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource(resource):
    print("request data",request.form)
    in_args, fields, body = parse_and_print_args()
    if request.method == 'GET':
        result = SimpleBO.find_by_template(resource, \
                                           in_args, fields)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'POST':
        print("in POST")
        formData = request.form.to_dict()
        result2 = SimpleBO.insert(resource,formData) #body defined above
        print("post result2 ", result2)
        return json.dumps(result2), 200, \
               {"content-type": "application/json; charset: utf-8"}
    else:
        return "hhhhhhMethod " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}

@app.route('/api/<resource>/<primary_key>', methods=['GET', 'DELETE', 'PUT'])
def primary_key(resource, primary_key):
    print("primary key before split ", primary_key)
    primary_key= primary_key.split("_")
    print("primary key, ", primary_key)
    in_args, fields, body = parse_and_print_args()
    if request.method == 'GET':
        in_args, fields, body = parse_and_print_args()
        print("in GET by PRIMARY KEY, fields ", fields)
        print("in GET by PRIMARY KEY, keys ", primary_key)
        print("in GET by PRIMARY KEY, in_args", in_args)
        print("in GET by PRIMARY KEY, body", body )
        result = SimpleBO.by_primary_key( resource, primary_key, fields )
        print("result", result)
        if result:
            print("in if result ")
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "NOT FOUND", 404
    if request.method == 'DELETE':
        template = SimpleBO.get_temp( resource, primary_key)
        result = SimpleBO.delete(resource, template)
        answer = print("Deleted: template ", template)
        print("result")
        print("result: ", result)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    if request.method == 'PUT':
        formData = request.form.to_dict()
        tmp = SimpleBO.get_temp(resource, primary_key )
        keys = tmp
        results = SimpleBO.put(resource, formData, tmp, primary_key[0])
        return results


# def put(table, primary_keys, fields, formData ):
#     q = "UPDATE" + table + "SET"
#     wc = SimpleBO.template_to_where_clause(formData)
#     q+= wc
#
#     nameLast = 'Mickey', nameFirst = 'Mouse'
#     +
#     WHERE
#      playerID = '111111103';


@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', 'POST'])
def primary_key_related_resource(resource, primary_key, related_resource):
   print("primary_key_related_resource primary_key, ", primary_key)
   primary_key = primary_key.split("_")

   print("resources/related_resources", resource, related_resource, primary_key)
   print("primary_key_related_resource primary_key after split, ", primary_key)
   in_args, fields, body = parse_and_print_args()

   if request.method == 'GET':
       in_args, fields, body = parse_and_print_args()
       print("in GET by PRIMARY KEY, fields ", fields)
       print("in GET by PRIMARY KEY, keys ", primary_key)
       result= SimpleBO.by_primary_key_join(resource, related_resource, primary_key, fields)
       print("result", result)
       if result:
           print("in if result ")
           return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
       else:
           return "NOT FOUND", 404

   elif request.method == 'POST':
       print("in POST")
       formData = request.form.to_dict()
       result2 = SimpleBO.insert(related_resource, formData)  # body defined above
       print("post result2 ", result2)
       return json.dumps(result2), 200, \
              {"content-type": "application/json; charset: utf-8"}
   else:
       return "NOT FOUND"

@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_teammates(playerid):
    print("in get teammates")
    in_args, fields, body = parse_and_print_args()
    if request.method == 'GET':
        r= get_teammates_cool(playerid)
        print(json.dumps(r, indent=2))
        return json.dumps(r), 200, {'Content-Type': 'application/json; charset=utf-8'}

def get_teammates_cool(playerid):
    print("in get teammates cool  ")
    q = """
    select
    a.playerid, b.playerid, 
    (select nameLast from people where people.playerid=b.playerid) as nameLast,
    (select nameFirst from people where people.playerid=b.playerid) as nameFirst,
    a.teamid, min(a.yearid), max(a.yearid),
    count(*) as total_seasons,
    group_concat(concat('[',a.teamid,',',a.yearid,']')) as team_year_list
    from
        appearances as a join appearances as b
    on 
        a.teamid=b.teamid and a.yearid=b.yearid
    where
        a.playerid > b.playerid
    and
        a.playerid = %s
    group by a.playerid, b.playerid, a.teamid"""
    print(q)
    result = SimpleBO.run_q(q, (playerid), True)
    return result

@app.route('/api/people/<playerid>/career_stats', methods=['GET'])
def career_stats(playerid):
    if request.method == 'GET':
        q = "SELECT * FROM roster where playerid = %s"
        result = SimpleBO.run_q(q, (playerid), True)
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
    else:
        return "THIS IS WHERE CAREER STATS GOES ", 404


if __name__ == '__main__':
    app.run()