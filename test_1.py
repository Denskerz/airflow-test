import logging
import random
import string

import datetime as dt
import airflow
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _write_file(count, length, out):
    logging.info("Information generation...")
    info = []
    string_ = []
    for n in range(int(count)):
        string_.append('')
        for i in range(int(length)):
            string_[n] += ' ' + random.choice(string.digits)
        #info.append(string_)
    logging.info("The list was created.")
    with open(out, "a") as f:
        logging.info("The file was opened.")
        for i in range(int(count)):
            f.write(f'{string_[i]}\n')
    logging.info("All the information was recorded in a file.")
        
with DAG(
    dag_id = "TransferFile",
    start_date = dt.datetime(2023, 11, 16),
    schedule = None
) as dag:
    
    create_file = BashOperator(
        task_id = "create_file",
        bash_command = 'echo "pipeline" > ~/file.txt',
        dag = dag,
    )
    
    write_file = PythonOperator(
        task_id="write_file",
        python_callable = _write_file,
        op_kwargs = {
            'count':'20000',
            'length':'45',
            'out':'/Users/denis/file.txt'
        },
        dag = dag,
    )

    transfer_file = BashOperator(
        task_id = "transfer_file",
        bash_command = 'cp ~/file.txt ~/Desktop/airflow-tutorial/',
        dag = dag,
    )

    rename_file = BashOperator(
        task_id = "rename_file",
        bash_command = 'mv ~/Desktop/airflow-tutorial/file.txt ~/Desktop/airflow-tutorial/New.txt',
        dag = dag,
    )

    delete_old_file = BashOperator(
        task_id = "delete_old_file",
        bash_command = 'rm ~/file.txt',
        dag = dag,
    )
    
    notify = BashOperator(
        task_id = "notify",
        bash_command = 'echo "File has been created."',
        dag = dag,
    )

    create_file >> write_file >> transfer_file >> rename_file >> delete_old_file >> notify
