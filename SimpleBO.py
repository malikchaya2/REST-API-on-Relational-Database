#foreign key only for batting to people

import pymysql
import json
from operator import itemgetter
from itertools import zip_longest as zip_longest


cnx = pymysql.connect(host='localhost',
                              user='root',
                              password='rootboot',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch = False) -> object:
    print("in run")
    cursor = cnx.cursor()
    if args != None:
        cursor.execute(q, args)
    else:
        cursor.execute(q)
    print("cursor execute ")
    # insertstr = "Success inserting {}".format(args[0])
    if fetch:
        print("in fetch ")
        result = cursor.fetchall()
        print("RUN Q1 ", result)
    else:
        print("in else ")
        result = "Success inserting {}".format(args[0])
        print("RUN Q2 ", result)
    cnx.commit()

    # if args:
    #     result = "Success inserting {}".format(args[0])
    #     print("RUN Q2 ", result)
    # else:
    #     result = cursor.fetchall()
    #     print("RUN Q1 ", result)
    #
    # cnx.commit()
    return result

def get_key_columns(table, key ):
    # This is MySQL specific and relies on the fact that MySQL returns the keys in
    # based on seq_in_index
    print("in get key columns ")
    q = "show keys FROM " + table
    print("q  ", q )
    cursor = cnx.cursor()
    cursor.execute(q)
    result = cursor.fetchall()
    print ("results  ", result )
    keys = [(r['Column_name'], r['Seq_in_index']) for r in result]
    keys = sorted(keys, key=itemgetter(1))
    keys = [k[0] for k in keys]
    print("keys", keys )
    return keys

def by_primary_key(table, key, fields) -> object:
    key_columns = get_key_columns(table, key )
    tmp = dict(zip(key_columns, key))
    result = find_by_template1(table, tmp, fields)
    return result

def get_temp(table, key):
    print("in getTemp")
    key_columns = get_key_columns(table, key)
    tmp = dict(zip(key_columns, key))
    return tmp

def template_to_where_clause1( t):
    print("template_to_where_clause1 t = ", t)
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"


    if s != "":
        s = "WHERE " + s

    return s


def template_to_where_clausejoin( t, table):
    print("template_to_where_clausejoin t = ", t)
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += table + "."+ k+ " = '" + v + "'"
    if s != "":
        s = "WHERE " + s;

    return s


def template_to_where_clause(t):
    print("template_to_where_clause t = ", t)
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"

    if s != "":
        s = "WHERE " + s;

    return s

def find_by_template(table, template, fields=None):
    print("in template, template = " , template)
    wc = template_to_where_clause(template)
    print("wc ", wc )
    q = "select "
    if fields == None:
        q += '*'
    else:
        q += fields[0]
    q+= " from " + table + " " + wc

    print("in template, q= ", q )
    result = run_q(q, None, True)
    return result

def find_by_template1(table, template, fields=None):
    print("in template1, template1 = " , template)
    # wc = template_to_where_clausejoin(template, table)
    wc = template_to_where_clause1(template)

    print("wc ", wc )
    q = "select "
    if fields == None:
        q += '*'
    else:
        q += fields[0]
    q+= " from " + table + " " + wc

    print("in template, q= ", q )
    result = run_q(q, None, True)
    return result

#post
def insert(table, row):
        print("in insert")
        print("row", row)
        print("keys", row.keys())
        keys = row
        q = "INSERT into " + table + " "
        s1 = list(keys)
        s1 = ",".join(s1)

        q += "(" + s1 + ") "

        v = ["%s"] * len(keys)
        v = ",".join(v)

        q += "values(" + v + ")"
        print("query ", q)
        print("param before tuple", row.values())
        params = tuple(row.values())
        print("params ", params)
        result = run_q(q, params, False)
        return result


def delete(table, template):
    print("IN DELETE ")
    print("template, ", template)
    # I did not call run_q() because it commits after each statement.
    # I run the second query to get row_count, then commit.
    # I should move some of this logic into run_q to handle getting
    # row count, running multiple statements, etc.
    where_clause = template_to_where_clause(template)
    print("where clause", where_clause)
    q1 = "delete from " + table + " " + where_clause + ";"
    q2 = "select row_count() as no_of_rows_deleted;"
    print("q1 ", q1)
    print ("q2, ", q2)
    cursor = cnx.cursor()
    cursor.execute(q1)
    cursor.execute(q2)
    result = cursor.fetchone()
    cnx.commit()
    return result

def by_primary_key_join(table, table2, key, fields) -> object:
   print("primary_key_join key, ", key )
   key_columns = get_key_columns(table, key )
   tmp = dict(zip(key_columns, key))
   print("primary_key_join, tmp ", tmp)
   result= find_by_template_join(table, table2,  key_columns, tmp, fields)
   return result



def find_by_template_join(table, table2, keys, template, fields=None):
   print("in template, templatejoin = " , template)
   wc = template_to_where_clausejoin(template, table)
   print("wc ", wc )
   q = "select "
   if fields == None:
       q += '*'
   else:
       q += fields[0]
   #INNER JOIN table2 t2 ON t1.h_id = t2.h_id
   primarykey = keys[0]
   q += " from " + table + " join " + table2 + " on " + table + "." + primarykey + " = " + table2 + "." + primarykey + " " + wc
   print("in template, q= ", q )
   result = run_q(q, None, True)
   return result

def put(resource, formData, tmp, primary_key):
    q = "UPDATE " + resource + " SET "
    s = ""

    print("q, ", q)
    for (k, v) in formData.items():
        if s != "":
            s += " , "
        s += k + "='" + v + "'"
    q += s

    print("q, ", q)
    wc = template_to_where_clause1(tmp)
    q += " " + wc
    # query good!!!
    print("q, ", q)
    print("wc, ", wc)
    cursor = cnx.cursor()
    cursor.execute(q)
    cnx.commit()
    result = "Success updating "+ primary_key + " to " + json.dumps(formData)
    print("result", result)
    return result