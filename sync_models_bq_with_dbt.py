# Sync BigQuery tables with local dbt project models.

import os
import glob
import re
from google.cloud import bigquery

# TODO: Enter the path to JSON key for auth in gcloud via service account.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:\\Users\\ivantoriya\\Work\\eapteka\\keys\\eapteka-1033-a182ce491875.json'

# TODO: Enter 'project.dataset' in BigQuery that you want to sync with dbt project folder.
dataset_id = 'eapteka-1033.dbt_itoriya'

# TODO: Enter the path to dbt models folder.
path_to_dbt_project_models = '//wsl$/Ubuntu-20.04/home/ivantoriya/GitHub/eapteka-dbt/models/**/*.sql'


client = bigquery.Client()

def dbt_models_name():

    models = [f for f in glob.glob(path_to_dbt_project_models, recursive=True)]
    models_names = []

    for f in models:
        filename = os.path.basename(f)
        models_names.append(re.search('(.*)\.', filename).group(1))

    return models_names


def bq_tables():

    tables = client.list_tables(dataset_id)  # Make an API request.

    tables_names = []
    for table in tables:
        tables_names.append(table.table_id)
    return tables_names


def remove_tables_from_bq():

    bq_tables_list = bq_tables()
    dbt_models_list = dbt_models_name()

    for table in bq_tables_list:
        if table not in dbt_models_list:
            # If the table does not exist, delete_table raises
            # google.api_core.exceptions.NotFound unless not_found_ok is True.
            client.delete_table(dataset_id + '.' + table, not_found_ok=False)  # Make an API request.
            print("Deleted table '{}'.".format(table))


if __name__ == '__main__':
    remove_tables_from_bq()
