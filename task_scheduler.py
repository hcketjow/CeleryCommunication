# Aby monitorować wyniki nalezy uruchomić: 
# 1. celery -A task_scheduler beat --loglevel=info
# 2. celery -A task_scheduler:app worker --loglevel=info
from celery import Celery
import time
import sqlite3
import json
from datetime import datetime
# Inicjalizacja Celery
app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Schedule the my_task task to run every 5 seconds using Celery beat
    sender.add_periodic_task(5.0, my_task.s(), name='my_task', expires=10)

def fetch_data_from_database():
    # Connect to the SQLite database
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    # Fetch data from the table 'data_table'
    cursor.execute('SELECT * FROM nagrody_forms_korespondencjapisf WHERE write_CRM IS NULL OR write_CRM = 0')
    data = cursor.fetchall()
    # Close the database connection
    conn.close()
    # Convert the data to a list of dictionaries
    columns = [desc[0] for desc in cursor.description]
    data_list = [dict(zip(columns, row)) for row in data]
    return data_list

def update_w_CRM(id_crm):
    # Connect to the SQLite database
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    # Update the 'your_column' to TRUE (1) for all rows in 'your_table'
    cursor.execute("UPDATE nagrody_forms_korespondencjapisf SET write_CRM = 1 WHERE id = "+str(id_crm))
    # Commit the changes and close the connection
    conn.commit()
    conn.close()


@app.task
def my_task():
    print("Task started.")
    data = fetch_data_from_database()
    if json.dumps(data) != "[]":
        with open("data.json", "a") as file:
            result = "Zapisano do CRM: " +datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            file.write(result + "\n")
            json.dump(data, file)
            file.write("\n")
    for row in data:
        if(row.get('write_CRM') == None or row.get('write_CRM') ==
        0):
            updated_data = update_w_CRM(row.get('id'))
            print(updated_data)
            # połącz się z CRM i wysłać zawarość komunikatu
        else:
            print("Brak danych dla CRM")
    return data

if __name__ == '__main__':
    app.worker_main(argv=['worker', '--beat'])
