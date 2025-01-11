from postgre_fun import * 
import datetime
import csv
import os
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="history", help="the export csv dir path")
args = parser.parse_args()
if __name__ == "__main__":
    date_time = datetime.datetime.now()
    date_time = datetime.datetime(2011,8,9)
    print("start from", date_time)
    
    # t.start()
    while date_time >= datetime.datetime(1990,1,1):
        print("now at date_time", date_time)

        q_res = []
        twse_byte, tpex_byte, done = None, None, False
        # while q_res is None or len(q_res)<1:
        q_res = query_data("date", ['twse', 'tpex', 'done'], {'date':[str(date_time.date())]})
        if q_res is not None:
            try:
                twse_byte, tpex_byte, done = q_res[0]
            except:
                print(q_res)
        else:
            print("date is empty")
            exit()
        # print(db_byte2str(twse_byte))
        date_str = str(date_time).split(' ')[0].replace("-", "_")
        # print(date_str)
        export_name_twse = date_str + "_twse.csv"
        export_name_tpex = date_str + "_tpex.csv"
        print(os.path.join(args.output_dir, export_name_twse))
        
        with open(os.path.join(args.output_dir, export_name_twse), "w") as f:
            f.write(db_byte2str(twse_byte))

        with open(os.path.join(args.output_dir, export_name_tpex), "w") as f:
            f.write(db_byte2str(tpex_byte))

            
        # exit()
        date_time = date_time - datetime.timedelta(days=1)