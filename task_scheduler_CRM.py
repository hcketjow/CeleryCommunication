# Aby monitorować wyniki nalezy uruchomić: 
# 1. celery -A task_scheduler_CRM beat --loglevel=info
# 2. celery -A task_scheduler_CRM:app worker --loglevel=info
from celery import Celery
import time
import mysql.connector
import sqlite3
import json
from datetime import datetime
# Inicjalizacja Celery
app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Schedule the my_task task to run every 5 seconds using Celery beat
    sender.add_periodic_task(5.0, task_send_to_CRM.s(), name='task_send_to_CRM', expires=10)
    sender.add_periodic_task(5.0, task_send_to_ISSW.s(), name='task_send_to_ISSW', expires=10)
#dodawanie JRWA
def fetch_and_send_data_to_CRM():
    # Connect to sqlite3 from ISSW 2.0
    connection = sqlite3.connect('db.sqlite3')
    # Connect to the MySQL database
    conn = mysql.connector.connect(
        user="username",
        password="password",
        host="host_number",
        port=0000,
        database="name_of_database"
    )
    cursor = conn.cursor()
    cursor_ISSW2 = connection.cursor()
    
    # Choose data which are from the ISSW 2.0 database sqlite3
    cursor_ISSW2.execute("SELECT id, tematwiado, tresc_wiadomosci, wnioskodawca_id, kierunek_korespondencji, data_wyslania_korespondencji, JRWA FROM korespondencja_korespondencjapisf WHERE write_CRM IS NULL OR write_CRM = 0")
    rows = cursor_ISSW2.fetchall()

    for row in rows:
        id = row[0] 
        tytul_wiadomosci = "'" + row[1] + "'"
        tresc_wiadomosci = "'" + row[2] + "'"
        imieinazwisko = row[3] 
        kierunek_korespondencji = row[4]
        data_wyslania_korespondencji = "'" + row[5] + "'"
        kierunek = "'Przychodząca'" if kierunek_korespondencji == 0 else "'Wychodząca'"
        wniosek_JRWA = row[6]

        cursor.execute(f"SELECT projectid FROM vtiger_project WHERE projectname = '{wniosek_JRWA}';")
        idcrmidprojektu = cursor.fetchone()[0]

        cursor.execute(f"SELECT linktoaccountscontacts FROM vtiger_project WHERE projectname = '{wniosek_JRWA}';")
        idcrmidbeneficjenta = cursor.fetchone()[0]
        cursor.execute("SELECT @idTabelavtiger_crmentity AS 'id_CRM_entity';")
        id_CRM_entity = cursor.fetchone()[0]

        try:
            #Gdzie 134 jest uzytkownikiem systemowym
            cursor.execute("INSERT INTO vtiger_crmentity(smcreatorid, smownerid, modifiedby, setype, createdtime, modifiedtime) "
                        f"VALUES(134, 134, 134, 'Koresponzbeneficj', {data_wyslania_korespondencji}, {data_wyslania_korespondencji});")
            idTabelavtiger_crmentity = cursor.lastrowid
        
            cursor.execute("INSERT INTO u_yf_koresponzbeneficj (koresponzbeneficjid, tematwiado, treswiad, projkoresp, issw_imieinazwisko, issw_kierunek, beneficjent) " \
                    f"VALUES ({idTabelavtiger_crmentity}, {tytul_wiadomosci}, {tresc_wiadomosci}, {idcrmidprojektu}, {imieinazwisko}, {kierunek}, {idcrmidbeneficjenta});")
            conn.commit()
            cursor_ISSW2.execute(f"UPDATE korespondencja_korespondencjapisf SET write_CRM = 1 WHERE id = {id}")
            connection.commit()
            num_rows_added = cursor_ISSW2.rowcount
            if num_rows_added == 1:
                with open("log.txt", "a") as file:
                    result = "Zapisano do CRM wpis o numerze id: " +str(id)+" "+datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    file.write(result + "\n")   
        except Exception as e:
            print("Error:", e)

def fetch_and_send_data_to_ISSW():
    # Connect to sqlite3 from ISSW 2.0
    conn_ISSW = sqlite3.connect('db.sqlite3')
    # # Connect to the MySQL database
    conn_maria_DB = mysql.connector.connect(
        user="username",
        password="password",
        host="host_number",
        port=0000,
        database="name_of_database"
    )
    cursor_Maria_DB = conn_maria_DB.cursor()
    cursor_ISSW2 = conn_ISSW.cursor()

    sql_query = """SELECT uvk.koresponzbeneficjid, uvk.tematwiado, uvk.treswiad, vp.projectname, CONCAT(vu.first_name, " ", vu.last_name) AS imieinazwisko_CRM
    FROM u_yf_koresponzbeneficj uvk, vtiger_crmentity vc, vtiger_users vu, vtiger_project vp 
    WHERE uvk.koresponzbeneficjid = vc.crmid 
    AND vc.smcreatorid = vu.id 
    AND uvk.projkoresp = vp.projectid"""

    cursor_Maria_DB.execute(sql_query)
    rows = cursor_Maria_DB.fetchall()

    for row in rows:
        koresponzbeneficjid = row[0] 
        tematwiado = "'" + row[1] + "'"
        treswiad = "'" + row[2] + "'"
        data_otrzymania_koresp = "'" + datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "'"
        projectname = row[3] #JRWA
        imieinazwisko_CRM = "'" + row[4] + "'"

        try:
            cursor_ISSW2.execute("INSERT INTO korespondencja_korespondencjapisf (tematwiado, tresc_wiadomosci, JRWA_id, opiekun_PISF, data_otrzymania_korespondencji)" \
                    f"VALUES ({tematwiado}, {treswiad}, {projectname}, {imieinazwisko_CRM}, {data_otrzymania_koresp});")
            conn_ISSW.commit()
            cursor_Maria_DB.execute(f"UPDATE u_yf_koresponzbeneficj SET write_issw = 1 WHERE koresponzbeneficjid = {koresponzbeneficjid}")
            num_rows_added = cursor_Maria_DB.rowcount
            if num_rows_added == 1:
                with open("log_MariaDB.txt", "a") as file:
                    result = "Zapisano do ISSW wpis o numerze id: " +str(koresponzbeneficjid)+" "+datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    file.write(result + "\n")   
        except Exception as e:
            print("Error:", e)

@app.task
def task_send_to_CRM():
    print("Rozpoczęto przekazywanie komunikatów do CRM.")
    data = fetch_and_send_data_to_CRM()
    return data

@app.task
def task_send_to_ISSW():
    print("Rozpoczęto przekazywanie komunikatów do ISSW")
    data = fetch_and_send_data_to_ISSW()
    return data

if __name__ == '__main__':
    app.worker_main(argv=['worker', '--beat'])
