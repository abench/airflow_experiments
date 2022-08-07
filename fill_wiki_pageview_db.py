# Create list of downloaded wikipageview statisctics
import os
import sqlite3

def create_date_string(filename:str)->str:
    splitted_file_name = os.path.splitext(filename)[0].split("_")
    print(splitted_file_name)
    # date time format for sqlite YYYY-MM-DD HH:MM "{04:}_{02:}_{02:} {02:}".
    date_string = "{:04}_{:02}_{:02} {:02}:00".format(int(splitted_file_name[2]),
                                                      int(splitted_file_name[3]),
                                                      int(splitted_file_name[4]),
                                                      int(splitted_file_name[5]))
    return date_string


data_directory = "/opt/data"
db_file_path = "/opt/data/wikipageviews.sqlite"
file_list = os.listdir(data_directory)
insert_sql = """
        insert into page_views (views_num, page_name, view_date) values( ?, ? ,?);
"""

select_sql = """
        select * from page_views;
"""

conn = sqlite3.connect(db_file_path)
cursor = conn.cursor()



for cur_file in file_list:
    if cur_file.endswith(".txt") and cur_file.startswith("wikipageviews_statistics_") :
        print(cur_file)
        date_string = create_date_string(cur_file)
        print(date_string)
        with open(os.path.join(data_directory,cur_file),"r") as f:
            file_data = f.readlines()
            for line in file_data:
                data_record = line.strip().split(" ")
                data_record.append(date_string)
                print(data_record)
                cursor.execute(insert_sql, data_record)
                print(cursor.rowcount)

conn.commit()
#conn.execute(select_sql)

#print(cursor)

conn.close()
# cycle on files . Extract statistics data and put them into list/dictionary

# insert extracted data to database