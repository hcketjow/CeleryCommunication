# To monitor the results you need to run:
# 1. celery -A task_scheduler_CRM beat --loglevel=info
# 2. celery -A task_scheduler_CRM:app worker --loglevel=info
from celery import Celery
import time
import mysql.connector
import sqlite3
import json
from datetime import datetime
# Create a celery task on server redis
app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Schedule the my_task task to run every 5 seconds using Celery beat
    sender.add_periodic_task(5.0, task_send_to_MariaDB.s(), name='task_send_to_MariaDB', expires=10)
    sender.add_periodic_task(5.0, task_send_to_sqlite3.s(), name='task_send_to_sqlite3', expires=10)
def fetch_and_send_data_to_MariaDB():
    # Connection to the sqlite3
    connection = sqlite3.connect('db.sqlite3')
    # Connect to MySQL/MariaDB database
    conn = mysql.connector.connect(
        user="username",
        password="password",
        host="host_number",
        port=0000,
        database="name_of_database"
    )
    # Cursor to the mysql database
    cursor = conn.cursor()
     # Cursor to the mysql sqlite
    cursor_ISSW2 = connection.cursor()
    
    # Choose names of the columns you want to execute from sqlite3
    cursor_ISSW2.execute("SELECT id, name, surname, age, direction_number, middle_name, JRWA FROM name_of_the_sqlite3_table WHERE column_name_CRM IS NULL OR column_name_CRM = 0")
    # Assign query to the one variable
    rows = cursor_ISSW2.fetchall()
    # Iterate through columns form the 'cursor_ISSW2'
    for row in rows:
        id = row[0] 
        name = "'" + row[1] + "'"
        surname = "'" + row[2] + "'"
        age = row[3] 
        direction_number = row[4]
        middle_name = "'" + row[5] + "'"
        direction = "'Incoming'" if direction_number == 0 else "'Outgoing'"
        application_JRWA = row[6]

        # Line to execute other tables if it is needed
        cursor.execute(f"SELECT column_name_1 FROM table_name WHERE projectname = '{application_JRWA}';")
        idcrmidprojektu = cursor.fetchone()[0]

        cursor.execute(f"SELECT column_name_2 FROM table_name WHERE projectname = '{application_JRWA}';")
        idcrmidbeneficjenta = cursor.fetchone()[0]
        cursor.execute("SELECT @idTabelatable_name_3 AS 'id_CRM_entity';")
        id_CRM_entity = cursor.fetchone()[0]

        #Here we insert the data into the table
        try:
            #Where 134 is system user
            cursor.execute("INSERT INTO table_name_3(smcreatorid, smownerid, modifiedby, setype, createdtime, modifiedtime) "
                        f"VALUES(134, 134, 134, 'Koresponzbeneficj', {direction_number}, {direction_number});")
            idTabelatable_name_3 = cursor.lastrowid
            # Inserting into MariaDB/MySQL tables
            cursor.execute("INSERT INTO name_of_the_mariaDB_table (id, names, surnames, projkoresp, age, directions_2, beneficiary) " \
                    f"VALUES ({idTabelatable_name_3}, {name}, {surname}, {idcrmidprojektu}, {age}, {direction}, {idcrmidbeneficjenta});")
            # Commiting the insertion
            conn.commit()
            # Update values from the table 'name_of_the_sqlite3_table'
            cursor_ISSW2.execute(f"UPDATE name_of_the_sqlite3_table SET column_name_CRM = 1 WHERE id = {id}")
            # Commiting the changes
            connection.commit()
            # Check number of rows which were added to the other database
            num_rows_added = cursor_ISSW2.rowcount
            if num_rows_added == 1:
                # If the transfer of the data went properly, then inform me in log.txt
                with open("log.txt", "a") as file:
                    result = "Saved into CRM entry number id: " +str(id)+" "+datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    file.write(result + "\n")   
        except Exception as e:
            print("Error:", e)

def fetch_and_send_data_to_sqlite3():
    # Connection to sqlite3
    conn_ISSW = sqlite3.connect('db.sqlite3')
    # Connection to the MySQL database
    conn_maria_DB = mysql.connector.connect(
        user="username",
        password="password",
        host="host_number",
        port=0000,
        database="name_of_database"
    )
    # Create the cursor for both databases
    cursor_Maria_DB = conn_maria_DB.cursor()
    cursor_ISSW2 = conn_ISSW.cursor()

    # Create SQL query from the 'name_of_the_mariaDB_table'
    sql_query = """SELECT uvk.id, uvk.name, uvk.surname, vp.projectname, CONCAT(vu.first_name, " ", vu.last_name) AS age_CRM
    FROM name_of_the_mariaDB_table uvk, table_name_3 vc, table_name_users vu, table_name vp 
    WHERE uvk.koresponzbeneficjid = vc.crmid 
    AND vc.smcreatorid = vu.id 
    AND uvk.projkoresp = vp.projectid"""

    # Execute the 'sql_query'
    cursor_Maria_DB.execute(sql_query)
    # Fetch all the data from the MariaDB which were set from the 'sql_query'
    rows = cursor_Maria_DB.fetchall()

    # Iterate through all the row in rows from the MariaDB database
    for row in rows:
        koresponzbeneficjid = row[0] 
        name = "'" + row[1] + "'"
        surname = "'" + row[2] + "'"
        get_date_koresp = "'" + datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "'"
        projectname = row[3]
        age_CRM = "'" + row[4] + "'"

        # Inserting data to the tables
        try:
            cursor_ISSW2.execute("INSERT INTO name_of_the_sqlite3_table (name, surname, JRWA_id, get_date_koresp, age_CRM)" \
                    f"VALUES ({name}, {surname}, {projectname}, {get_date_koresp}, {age_CRM});")
            # Commit all the exectution of the 'cursor_ISSW2'
            conn_ISSW.commit()
            # Create functionality to update the second table
            cursor_Maria_DB.execute(f"UPDATE name_of_the_mariaDB_table SET write_issw = 1 WHERE koresponzbeneficjid = {koresponzbeneficjid}")
            # Check number of rows which were added to the other database
            num_rows_added = cursor_Maria_DB.rowcount
            # If the transfer of the data went properly, then inform me in log_MariaDB.txt
            if num_rows_added == 1:
                with open("log_MariaDB.txt", "a") as file:
                    result = "Zapisano do ISSW wpis o numerze id: " +str(koresponzbeneficjid)+" "+datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    file.write(result + "\n")   
        except Exception as e:
            print("Error:", e)

# Create a task for sending data from sqlite3 to MariaDB
@app.task
def task_send_to_MariaDB():
    # Message to keep track if the data are transfering
    print("The transfer of messages to the MariaDB has begun.")
    # Name of the first method
    data = fetch_and_send_data_to_MariaDB()
    return data

# Create a task for sending data from MariaDB to sqlite3
@app.task
def task_send_to_sqlite3():
    # Message to keep track if the data are transfering
    print("Message transmission to sqlite3 has begun")
     # Name of the second method
    data = fetch_and_send_data_to_sqlite3()
    return data

# Create a worker for transfering the data
if __name__ == '__main__':
    app.worker_main(argv=['worker', '--beat'])
