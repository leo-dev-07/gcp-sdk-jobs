import os
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Administrador\Downloads\projeto-rox-data-eng-bf3c3b56cb7d.json'

#Instancia um objeto da classe storage.cliente
storage_cli = storage.Client(project='projeto-rox-data-eng')

#Instancia um objeto da classe biigquery.cliente
bq_cli = bigquery.Client(project='projeto-rox-data-eng')

#Função de criiação de bucket
def create_bucket(name):
    try:
        bucket = storage_cli.bucket(name)
        bucket.location = 'us-east1'
        bucket = storage_cli.create_bucket(bucket)  
        print('bucket criado {}'.format(bucket))

    except Exception as e:
        print(e)
        return False
#Função de carregamento de aquivos
def upload_files(blob_name,file_path,bucket_name):
    try:
        bucket = storage_cli.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False
#Função que devolve uma lista com todas as URIs do bucket
def list_blobs(bucket_name):
    blobs = storage_cli.list_blobs(bucket_name)
    lst=[]
    print('listing blobs')
    for blob in blobs:
        if blob.name[-4:] == '.csv':
            lst.append('gs://'+bucket_name+'/'+blob.name)
            print('gs://'+bucket_name+'/'+blob.name)
    return lst

#Infere o schema com base nos dados da tabela
def schema_detection_bq():
    jobConfig = bigquery.LoadJobConfig()
    #jobConfig.skip_leading_rows = 1
    jobConfig.field_delimiter = ';'
    jobConfig.source_format = bigquery.SourceFormat.CSV
    jobConfig.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    jobConfig.autodetect = True
    return jobConfig

#Cria um dataset no BigQuery
def create_dataset_bq(dataset_id):
    # Construct a BigQuery client object.

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "us-east1"

    dataset = bq_cli.create_dataset(dataset, timeout=5)  # Make an API request.
    print("Created dataset {}.{}".format(bq_cli.project, dataset.dataset_id))

#Cria uma tabela e carrega dados vindos do bucket.
def create_and_load_table_bq(dataset_name,table_name,uri):
    print('Creating table')
    job_conf = schema_detection_bq()
    table_ref = bq_cli.dataset(dataset_name).table(table_name)
    print('Loading table')
    bigquery_job = bq_cli.load_table_from_uri(uri, table_ref, job_config=job_conf)
    bigquery_job.result()
    print("Created table {}.".format(bigquery_job.project))

#Carrega tabela para o bigQuery
def load_tb_bq(file_path,table_id):

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = bq_cli.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()

    table = bq_cli.get_table(table_id)

#Pega o nome dos arquivos do bucket para criar as tabelas com base neles
def get_file_name(bucket_name):
    blobs = storage_cli.list_blobs(bucket_name)
    lst=[]

    for blob in blobs:
        lst.append(blob.name)

    names=[]
    print('geting file names')

    for name in lst:
        print(name.split(r'/')[-1:][0])
        try:
            if name.split(r'/')[-1:][0].split('.')[-1:][0] != 'csv':
                pass
            else:
                #print(name.split(r'/')[:-1])
                names.append(name.split(r'/')[-1:][0][:-4].lower().replace('.','_'))
        except Exception as e:
            print(e)
            pass
    return names

# Função geral para criar tabelas direto no BQ vindo do bucket para múltiplos arquivos (também funciona para um único arquivo)
def batch_table_creation_bq(bucket_name,dataset_name):
    tab_names = get_file_name(bucket_name)
    blobs_list = list_blobs(bucket_name)
    index=0
    for name in tab_names:
        for blob in blobs_list:
            print(tab_names[index])
            create_and_load_table_bq(dataset_name, tab_names[index], blob)
            index+=1
        break

    return {'Status':200,
            'Msg':'Criação de tabela e inserssão feita com sucesso',
            'Tabelas criadas':tab_names,
            'Blobs':blobs_list}

#bucket = storage_cli.get_bucket('bike-sales-01062021')

#%%
#Este bloco dá os nome siniciais do projeto.
bk_name='bike-sales-01062021'
dataset = 'bike_sales_1'
x = get_file_name(bk_name)
x = batch_table_creation_bq(bk_name, dataset)
print(x)
#print(x)
#bucket = storage_cli.get_bucket(bk_name)
#%%
#get_file_name()
#create_table_bq('bike_sales_1','test_tab','gs://bike-sales-01062021/bike-data/input/Sales.Customer.csv')
