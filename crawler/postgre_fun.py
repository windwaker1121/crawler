import psycopg2
import os
import csv
import time

host = "172.128.0.2"
dbname = "stock"
user = "admin"
password = "admin"
sslmode = "allow"

# Construct connection string
connect_succes = False
while not connect_succes:
    try:
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        conn = psycopg2.connect(conn_string)
        print("Connection established")
        cursor = conn.cursor()
        connect_succes = True
    except:
        
        pass
    print(connect_succes)
    time.sleep(1)

def close():
    cursor.close()
    conn.close()

def get_cursor():
    return cursor 

def insert(table_name:str, properityes:list, values:list, PK:str=None):
    assert len(properityes) ==len(values), "the number of properityes and values are not equal "+str(len(properityes))+"-"+str(len(values))

    SQL = "INSERT INTO "+table_name+" "
    SQL+= "(" + ", ".join(properityes) + ") VALUES (" + ", ".join(["%s"]*len(properityes)) +")"

    # if len(properityes) == 1:
    #     SQL = SQL+str(tuple(properityes)).replace("'","").replace(",","") + " VALUES "
    #     SQL = SQL+str(tuple(values)).replace(",","")#.replace("'","")
    # else:
    #     SQL = SQL+str(tuple(properityes)).replace("'","") + " VALUES "
    #     SQL = SQL+str(tuple(values))#.replace("'","")
    if PK is not None:
        SQL = SQL+" ON CONFLICT DO NOTHING RETURNING "+PK+";"
    else:
        SQL = SQL+" ON CONFLICT DO NOTHING;"
    # print(SQL)
    cursor.execute(SQL, tuple(values))
    try:
        insert = cursor.fetchall()
        conn.commit()
        if len(insert)> 0:
            return insert[0][0]
    except Exception as e:
        print(e, "nothing to insert")
        conn.rollback()

    return None

def commit(): 
    conn.commit()

def rollback(): 
    conn.rollback()

def check_table(db_name, table_name):
    SQL= \
        "SELECT EXISTS(\
            SELECT * \
            FROM information_schema.tables \
            WHERE \
            table_name = '"+table_name+"'\
        );"
    cursor.execute(SQL)
    # print(SQL)
    results = cursor.fetchall() 
    return results[0][0]

def create_table(table_name, columns):
    SQL = "CREATE TABLE "+table_name+" ("
        
            # city            varchar(80),
            # temp_lo         int,           -- low temperature
            # temp_hi         int,           -- high temperature
            # prcp            real,          -- precipitation
            # date            date
    COLUMNS_STRING = []
    for c in columns:
        # print(c)
        COLUMNS_STRING.append(" ".join(c))
    SQL+= ",".join(COLUMNS_STRING)+");"
    try:
        cursor.execute(SQL)
        conn.commit()
        # print(SQL)
    except Exception as e:
        print(e)
        conn.rollback()

def add_column(table_name, col_name, tar_type):
    SQL="\
        ALTER TABLE "+table_name+"\
        ADD "+col_name+" "+tar_type+";"
    print(SQL)
    cursor.execute(SQL)
    
def alter_column(table_name, col_name, tar_type, ori_col=None):
    
    if ori_col in ['character', "character varying"]:
        SQL=\
        "ALTER TABLE "+table_name+"\
        ALTER COLUMN "+col_name+" TYPE "+tar_type+" USING (trim("+col_name+")::"+tar_type+");"
    else:
        SQL=\
        "ALTER TABLE "+table_name+"\
        ALTER COLUMN "+col_name+" TYPE "+tar_type+" USING ("+col_name+"::"+tar_type+");"
    print(SQL)
    cursor.execute(SQL)  
    print("ALTER col",col_name, tar_type)
    pass

def register_account(username, email, password, org):
    SQL= \
        "insert into account ( email, password, username, organization, activate)\
            values ("+",".join(map(lambda x: "'"+x+"'", [email, password, username, org, 'false']) )+"\
        ) RETURNING email;"
    print(SQL)
    try:
        print("in sql")
        cursor.execute(SQL)
        pk_of_new_row = cursor.fetchone()[0]
        print(pk_of_new_row, pk_of_new_row == email)
        if pk_of_new_row == email:
            conn.commit()
            return 0, "success"
        else:
            conn.rollback()
            return 1312, "account cmp error"
    except Exception as e:
        conn.rollback()
        msg = str(e)
        print('except', msg)
        if 'already' in msg and 'exists' in msg and email in msg:
            return 409, "account is exists"
        elif 'duplicate' in msg and 'violates' and username in msg:
            return 409, "account is exists"
    conn.rollback()
    return 1312, "unknow error"

def activate_account(email):
    SQL= \
        "\
            update account \
            set activate = true\
            where email = '"+email+"'\
        ;"
    print(SQL)
    try:
        print("in sql")
        cursor.execute(SQL)
        conn.commit()
        return 0, "account activate."

    except Exception as e:
        conn.rollback()
        msg = str(e)
        print(msg)
        return 1312, "activate falid."
    conn.rollback()
    
    return 1312, "unknow error"

def change_password(username, oldpassword, newpassword):
    SQL= "\
            update account \
            set password = '"+newpassword+"'\
            where username = '"+username+"' "
    if oldpassword is not None:
        SQL += " and password = '"+oldpassword+"'"
    SQL += "RETURNING username;"
    print(SQL)
    try:
        cursor.execute(SQL)
        update_username = cursor.fetchone()
        if update_username is not None:
            update_username = update_username[0]
        print(update_username)
        if update_username == username:
            conn.commit()
            return 200, "update sussecc"
        else:
            conn.rollback()
            return 401, "old password is not match"
    except Exception as e:
        msg = str(e)
        print(msg)
    conn.rollback()
    return 1312, "update error"

def get_user_info(username, usertype='user'):
    if usertype == 'admin':
        usertype_str = '_admin'
        activate_str = ''
        search_str = 'username'
    else:
        usertype_str = ""
        activate_str = 'and activate = true'
        search_str = 'username, email, organization, authority'
    SQL= \
        "\
            select "+search_str+" from account"+usertype_str+" \
            where username = '"+username+"'\
                "+activate_str+"\
        ;"
    cursor.execute(SQL)
    user_info = cursor.fetchone()
    print(user_info)
    return user_info

def query_data(table:str, cols:list, where:dict=None):
    SQL = "\
        select "
    COL = " "
    query_item = []
    for c in cols:
        query_item.append(c)
    COL += ",".join(query_item) 
    SQL += COL
    SQL += " from "+table+" "
    if where is not None:
        WHERE = "where "
        where_item = []
        for k, v in where.items():
            where_item.append(k+" in ('"+"','".join(v)+"') ")
        WHERE += ' and '.join(where_item)
        if len(where_item)>0:
            SQL += WHERE
    cursor.execute(SQL)
    try:
        query = cursor.fetchall()
        # print(query)
        if len(query)> 0:
            return query
    except Exception as e:
        return None

def update_data(table:str, data:dict, where:dict):
    SQL = "\
        update "+table+" "
    SET = "set "
    set_item = []
    for k, v in data.items():
        if k not in where.keys():
            set_item.append(k + " = '"+v+"' ")
    SET += ",".join(set_item) 
    SQL += SET

    WHERE = "where "
    where_item = []
    for k, v in where.items():
        where_item.append(k+" = '"+v+"' ")
    WHERE += ' and '.join(where_item)
    SQL += WHERE

    DIS = "AND "
    dis_item = []
    for k, v in data.items():
        if k not in where.keys():
            dis_item.append(k+" IS DISTINCT FROM '"+v+"' ")
    if len(dis_item) > 0:
        SQL += DIS + "("+" or ".join(dis_item)+")"
    SQL += " returning "+list(where.keys())[0]+";"
    print(SQL)
    cursor.execute(SQL)
    try:
        update = cursor.fetchall()
        print(update)
        conn.commit()
        if len(update)> 0:
            return update
    except Exception as e:
        print(e, "nothing to update")
        conn.rollback()

    return None

def delete_data(table, where):
    
    SQL = "\
        delete from "+table+" "
    
    WHERE = "where "
    where_item = []
    for k, v in where.items():
        where_item.append(k+" = '"+v+"' ")
    WHERE += ' and '.join(where_item)
    SQL += WHERE
    SQL += " returning "+list(where.keys())[0]+";"
    # print(SQL)
    cursor.execute(SQL)
    try:
        update = cursor.fetchall()
        # print(update)
        conn.commit()
        if len(update)> 0:
            return update
    except Exception as e:
        print(e, "nothing to delete")
        conn.rollback()

    return None

def search_user(search=None, orderby=None, pagedatas=10000, page=0):
    SQL = "\
        select username, email, organization, authority, activate\
        from account "
        
    if search is not None:
        SQL += "\nwhere "
        for k, v in search.items():
            SQL += k + " = '" +v+"'"

    if orderby is not None:
        SQL += "\norder by " + orderby.keys()[0]+" "+orderby.values()[0]

    # SQL += "OFFSET "+str(page*pagedatas)+" limit "+str(pagedatas)+";"
    SQL += ";"
    print(SQL)
    cursor.execute(SQL)
    results = cursor.fetchall() 
    # return results
    query = []
    for i, r in enumerate(results) :
        query.append(
            {
                # 'id':i+1,
                'username':     r[0],
                'email':        r[1],
                'organization': r[2],
                'authority':    r[3],
                'activate':     r[4],
            }
        )

    # print(results)
    return query

def update_file(table, key:dict, bytefile:dict):
    files = []
    for k, v in bytefile.items():
        files.append(k+"='"+v+"'")
    FILES = ",".join(files)
    SQL = "\
        UPDATE "+table+" SET "+FILES+" WHERE "+list(key.items())[0][0]+"='"+list(key.items())[0][1]+"';\
        INSERT INTO "+table+" ("+list(key.items())[0][0]+", "+list(bytefile.items())[0][0]+")\
            SELECT '"+list(key.items())[0][1]+"', '"+list(bytefile.items())[0][1]+"'\
            WHERE NOT EXISTS (SELECT 1 FROM "+table+" WHERE "+list(key.items())[0][0]+"='"+list(key.items())[0][1]+"');\
    "
    # print(SQL)
    # print(list(bytefile.items())[0][1])
    cursor.execute(SQL)
    try:
        conn.commit()
        return True
    except Exception as e:
        print(e, "no file to update")
        conn.rollback()
    return False

def get_file(table, key:dict, column_name):
    SQL = "\
        select "+column_name+" from "+table+" where "+list(key.items())[0][0]+"='"+list(key.items())[0][1]+"'\
    ;"
    print(SQL)
    # print(list(bytefile.items())[0][1])
    cursor.execute(SQL)
    try:
        res = cursor.fetchone()
        return str(psycopg2.Binary(res[0]))[1:-8]
    except Exception as e:
        print(e, "nothing get_file")
        conn.rollback()
    return None

def get_auth_type():
    SQL = "\
        select authority\
        from authority;"
        
    
    print(SQL)
    cursor.execute(SQL)
    results = cursor.fetchall() 

    # print(results)
    return [r[0] for r in results]

def login_check(username, password, usertype='user'):
    if usertype == 'admin':
        usertype_str = '_admin'
        activate_str = ''
    else:
        usertype_str = ""
        activate_str = 'and activate = true'
    SQL= \
        "SELECT EXISTS(\
            select * from account"+usertype_str+" \
            where username = '"+username+"' and password = '"+password+"'\
                "+activate_str+"\
        );"
    
    # print(SQL)
    try:
        cursor.execute(SQL)
        # results = cursor.fetchall() 

        is_login = cursor.fetchone()[0]
        print(is_login)
        if is_login:
            return 0
        else:
            return 401
    except Exception as e:
        msg = str(e)
        print(msg)
    return 1312
