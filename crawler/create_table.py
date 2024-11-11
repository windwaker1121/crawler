import psycopg2
import os
import csv
import time
from postgre_fun import check_table, create_table, add_column, alter_column, get_cursor, commit, rollback, close

# host = "172.128.0.2"
dbname = "stock"
# user = "admin"
# password = "admin"
# sslmode = "allow"

# connect_succes = False
# while not connect_succes:
#     try:
#         conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
#         conn = psycopg2.connect(conn_string)
#         print("Connection established")
#         cursor = conn.cursor()
#         connect_succes = True
#     except:
        
#         pass
#     print(not connect_succes)
#     time.sleep(1)

if __name__ == "__main__":
    # cursor.execute("CREATE TABLE inventory (id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER);")
    # Update connection string information
    # print("Finished creating table")

    # CREATE USER stadmin WITH PASSWORD 'stmed' SUPERUSER;
    # create database fl;
    cursor = get_cursor()
    schema_root = '/schema'
    for table in (os.listdir(schema_root)):
        print(table)
        table_name = table.replace(".csv", "")
        columns = []
        with open(os.path.join(schema_root, table) , newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for i, row in enumerate(spamreader):
                columns.append(row)
                print("Names" ,', '.join(row))

        if not check_table(dbname, table_name):
            # table not exists
            print("Create", table)
            create_table(table_name, columns)
        else:
            # table exists check column
            SQL=\
                "select column_name, data_type, character_maximum_length\
	                from information_schema.columns\
                    where table_schema = 'public' and \
                    table_name = '"+table_name+"' \
                ;"
            cursor.execute(SQL)
            results = cursor.fetchall()
            print(results)
            datas = {}
            for r in results:
                datas[r[0]] = {"type":r[1], 'props': r[2:]}
            print(datas)
            print(list(datas.keys()))
            for c in columns:
                tar_name = c[0].strip()
                if tar_name not in list(datas.keys()):
                    add_column(table_name, tar_name, c[1])
                    print(tar_name, "is not in table add to table")
                else:
                    print(tar_name, "in table check type")

                    if datas[tar_name]['type'] in ['character', 'character varying']:
                        ori_type = datas[tar_name]['type']+"("+str(datas[tar_name]['props'][0])+")"
                    else:
                        if c[1] == 'serial' and datas[tar_name]['type'] == 'integer':
                            ori_type = 'serial'
                        else:
                            ori_type = datas[tar_name]['type']
                    if ori_type != c[1]:
                        alter_column(table_name, tar_name, c[1])
                    else:
                        print("data type is equal")
            # pass
            # with open(os.path.join(schema_root, table) , newline='') as csvfile:
            #     spamreader = csv.reader(csvfile, delimiter=',')
                
            #     for i, row in enumerate(spamreader):
            #         print("Names" ,', '.join(row))


    # Insert some data into the table
    # insert('test', ("name",), ("banana",))
    # insert('test', ("name",), ("orange",))
    # insert('test', ("name",), ("apple",))
    
    print("Inserted 3 rows of data")
    
    # Clean up
    try:
        commit()
    except:
        rollback()
    close()
    pass