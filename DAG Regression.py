

# .................... Import Packages ...........................

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from datetime import datetime, timedelta

import glob
import pandas as pd
import csv

import pickle
import boto3

from sklearn.linear_model import LogisticRegression

# ..................... AmazonS3Client Class .......................

class AmazonS3Client(object):
    _cl = None
    def __init__(self):
        if not self._cl:
            self._cl = boto3.client('S3',
                aws_access_key_id='...',
                aws_secret_access_key='...',
                aws_session_token='...',
            )
        self.cl = self._cl

# ...................... Download and Upload ....................

def download(file_name, bucket_name, save_as=None):
    s3 = AmazonS3Client().cl
    if not save_as:
        save_as = file_name
    s3.download_file(bucket_name, file_name, save_as)


def upload(file_obj, file_name, bucket_name):
    s3 = S3ClientSingleton().cl
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_obj)


#.............. Task functions (task1, task2, task3, task4) .............

def task1():
    s3 = S3ClientSingleton().cl
    for s3_samp in s3.list_objects(Bucket='fariba.ub.training')['Contents']:
        filename = s3_samp['Key']
        download(filename, 'fariba.ub.training', filename)


def task2():
    file = glob.glob("train*.csv")
    data = []
    for myfile in file:
        df = pd.read_csv(myfile, index_col=None, header=0)
        data.append(df)
    df = pd.concat(data, axis=0, ignore_index=True)
    X = df.loc[:, df.columns != 'Species']
    y = df.loc[:, df.columns == 'Species']
    clf = LogisticRegression()
    clf.fit(X, y)
    with open('iris_trained_model.pkl', 'wb') as f:
        pickle.dump(clf, f)

def task3():
    download('predict.csv', 'fariba.ub.prediction')
    my_test_data = pd.read_csv('predict.csv', index_col=None, header=0)
    with open('iris_trained_model.pkl', 'rb') as f:
        clf_loaded = pickle.load(f)
    y_pred = clf_loaded.predict(my_test_data)
    my_test_data['Species'] = y_pred
    my_test_data.to_csv('_' + 'predict.csv')

def task4():
    with open('iris_trained_model.pkl', 'rb') as f1:
        upload(f1, 'iris_trained_model.pkl', 'fariba.ub.ml')
    with open('predict.csv', 'rb') as f2:
        myfiles = 'prediction_{}'.format(datetime.today())
        upload(f2, myfiles, 'fariba.ub.prediction')


#............... Arguments of the DAG ...................

args = {
    'owner': 'Fariba',
    'depends_on_past': False,
    'start_date': timezone.utcnow(),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# ................ My DAG ...................

dag = DAG(dag_id='MyDag',
          default_args=args,
          description="MyDAG in Final Assignment of This Course",
          schedule_interval='0 20 0 * 1')  # at 8 PM each Monday 

# ................... Tasks 1-4 .....................

op1 = PythonOperator(task_id='Task1',
    python_callable=task1,
    dag=dag)

op2 = PythonOperator(task_id='Task2',
    python_callable=task2,
    dag=dag)

op3 = PythonOperator(task_id='Task3',
    python_callable=task3,
    dag=dag)

op4 = PythonOperator(task_id='Task4',
    python_callable=task4,
    dag=dag)


# ........................ End ................................

op1 >> op2
[op2, op3] >> op4