from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
from sqlalchemy import create_engine
import boto3

default_args = {
    'owner': 'Alan Cafruni Gularte',
    "depends_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email": ["alangularte@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(default_args=default_args, schedule_interval=None, description="This pileline gets data from MongoDB, upload the CSV into AWS S3 Bucket and insert the filtered data into AWS RDS Postgres")
def mongodb_to_aws_s3_postgres():

    @task
    def extract_from_mongodb():
        import pymongo
        import pandas as pd
        mongodb_username = Variable.get("mongodb_username")
        mongodb_password = Variable.get("mongodb_password")
        mongodb_hostname = Variable.get("mongodb_hostname")
        client = pymongo.MongoClient(f'mongodb+srv://{mongodb_username}:{mongodb_password}@{mongodb_hostname}/ibge?retryWrites=true&w=majority (Links para um site externo.)')
        db = client.ibge
        pnad_collec = db.pnadc20203
        df = pd.DataFrame(list(pnad_collec.find()))
        df.to_csv("./pnadc20203.csv", index=False, encoding='utf-8', sep=';')
        return "./pnadc20203.csv"

    @task
    def upload_to_s3(file_name):
        aws_access_key_id = Variable.get("aws_access_key_id")
        aws_secret_access_key = Variable.get("aws_secret_access_key")
        aws_s3_name = Variable.get("aws_s3_name")

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        s3_client.upload_file(file_name,aws_s3_name,file_name[5:])

    @task
    def import_to_postgres(csv_file_path):
        import pandas as pd
        aws_postgres_host= Variable.get("aws_postgres_host")
        aws_postgres_port= Variable.get("aws_postgres_port")
        aws_postgres_username = Variable.get("aws_postgres_username")
        aws_postgres_password = Variable.get("aws_postgres_password")
        conn = create_engine(
            f'postgresql://{aws_postgres_username}:{aws_postgres_password}@{aws_postgres_host}:{aws_postgres_port}/postgres')
        df = pd.read_csv(csv_file_path,sep=";")
        df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == 'Mulher')]
        df['dt_inclusao_registro'] = datetime.today()
        df.to_sql(csv_file_path[5:-4], conn, index=False, if_exists="replace", method='multi', chunksize=1000)

    extract_from_mongodb = extract_from_mongodb()    
    upload_to_s3(extract_from_mongodb)
    import_to_postgres(extract_from_mongodb)

mongodb_to_aws_s3_postgres = mongodb_to_aws_s3_postgres()