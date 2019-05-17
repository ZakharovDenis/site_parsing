from google.oauth2 import service_account
from google.cloud import bigquery
from parsersuper import get_all_data, download_all_data


dataset, brands, countries, categories = get_all_data()

try:
    del dataset["Unnamed: 0"]
except KeyError:
    pass

credentials = service_account.Credentials.from_service_account_file(
    'silken-champion-237507-2ac985d9ef0c.json')
private_key='silken-champion-237507-2ac985d9ef0c.json'
project_id='silken-champion-237507'
client = bigquery.Client(credentials=credentials, project = project_id)

dataset.to_gbq('hatewait.product', if_exists='replace',
                    project_id=project_id, private_key=private_key)

countries.to_gbq('hatewait.country', if_exists='replace',
                    project_id=project_id, private_key=private_key)

brands.to_gbq('hatewait.brand', if_exists='replace',
                    project_id=project_id, private_key=private_key)

categories.to_gbq('hatewait.category', if_exists='replace',
                    project_id=project_id, private_key=private_key)
