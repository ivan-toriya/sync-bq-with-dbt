# Sync BigQuery tables with local dbt project models.

import os
import glob
import re

from google.cloud import bigquery

# TODO: Enter the path to JSON key for auth in gcloud via service account.
# You need "BigQuery Metadata Viewer" role or higher.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:\\...\\keys\\key.json'

# TODO: Enter 'project.dataset' in BigQuery that you want to sync with dbt project folder.
dataset_id = 'project_id.dbt_dataset_name'

# TODO: Enter the path to dbt models folder.
path_to_dbt_project_models = 'C:\\...\\dbt-project\\models\\**\\*.sql'


client = bigquery.Client()


def local_dbt_models() -> set:
    models = set(f for f in glob.glob(path_to_dbt_project_models, recursive=True))
    
    models_names = set()
    for f in models:
        filename = os.path.basename(f)
        models_names.add(re.search('(.*)\.', filename).group(1))
    
    print(f'You have {len(models_names)} models in local dbt project.')
    
    return models_names


def bq_models() -> set:
    tables = client.list_tables(dataset=dataset_id)  # Make an API request.
    table_names = set(table.table_id for table in tables)
    
    print(f'You have {len(table_names)} models in {dataset_id}.')
    
    return table_names


def remove_tables_from_bq():
    diff = bq_models().difference(local_dbt_models())
    
    if diff != set():
        for table in diff:
            client.delete_table(dataset_id + '.' + table, not_found_ok=False)  # Make an API request.
            print(f"{table} deleted from {dataset_id}")
    else:
        print('No tables to delete. Your models are in sync.')


if __name__ == '__main__':
    remove_tables_from_bq()
