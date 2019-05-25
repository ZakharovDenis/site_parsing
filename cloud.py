from google.oauth2 import service_account
from google.cloud import bigquery
from parsersuper import get_all_data, download_all_data, get_product_info
import pandas as pd
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings


credentials = service_account.Credentials.from_service_account_file(
    'credentials/silken-champion-237507-2ac985d9ef0c.json')
private_key='credentials/silken-champion-237507-2ac985d9ef0c.json'
project_id='silken-champion-237507'
client = bigquery.Client(credentials=credentials, project = project_id)

def get_token(file_path):
    with open(file_path) as file:
        token = file.read()
    return token
accountkey = get_token("credentials/azure_token.txt")
accountname = "siteparsing"
block_blob_service = BlockBlobService(account_name = accountname, account_key = accountkey)


def save_phones(dataset):
    dataset.to_gbq('hatewait.product', if_exists='replace',
                    project_id=project_id, private_key=private_key)

def save_counties(countries):
    countries.to_gbq('hatewait.country', if_exists='replace',
                    project_id=project_id, private_key=private_key)
def save_brands(brands):
    brands.to_gbq('hatewait.brand', if_exists='replace',
                    project_id=project_id, private_key=private_key)
def save_categories(categories):
    categories.to_gbq('hatewait.category', if_exists='replace',
                    project_id=project_id, private_key=private_key)

def save_all_google():
    dataset, brands, countries, categories = get_all_data()
    try:
        del dataset["Unnamed: 0"]
        del brands["Unnamed: 0"]
        del countries["Unnamed: 0"]
        del categories["Unnamed: 0"]
    except KeyError:
        pass
    save_brands(brands)
    save_categories(categories)
    save_counties(countries)
    save_phones(dataset)

def get_all_google():
    query='''
    SELECT b as Brand, Model, c as Category, Price, Co as Country, Link
    FROM
        (SELECT b, Model, c, Price, Co, Link
        FROM
            (SELECT b, Model, c, Price, Country, Link
            FROM 
                (SELECT b, Model, Category, Price, Country, Link
                FROM hatewait.product
                JOIN
                (SELECT id as idb, brand as b
                FROM hatewait.brand) as t1
                ON Brand=idb) as t3
            JOIN
            (SELECT id as idc, category as c
            FROM hatewait.category) as t2
            ON Category=idc) as t5
        JOIN
        (SELECT id as idco, country as co
        FROM hatewait.country) as t4
        ON Country=idco)
    '''
    site_data = pd.read_gbq(query, project_id=project_id, credentials=credentials, dialect="legacy")
    return site_data


def save_all_microsoft():
    try:
        sitedatacloud=pd.read_csv('site_data.csv', encoding='utf-8')
    except:
        sitedatacloud=get_product_info(write=True)
    try:
        del sitedatacloud["Unnamed: 0"]
    except KeyError:
        pass
    block_blob_service.create_container('sitedatacloud')
    block_blob_service.create_blob_from_path(
    'sitedatacloud',
    'sitedata.csv',
    'site_data.csv',
    content_settings=ContentSettings(content_type='application/CSV'))

def get_all_microsoft():
    block_blob_service.get_blob_to_path('sitedatacloud', 'sitedata.csv', 'out-data.csv')
    site_data = pd.read_csv("out-data.csv")
    return site_data

def save_all(cloud):
    if cloud=='bigquery':
        save_all_google()
    elif cloud=='azure':
        save_all_microsoft()

def get_all(cloud):
    if cloud=='bigquery':
        return get_all_google()
    elif cloud=='azure':
        return get_all_microsoft()