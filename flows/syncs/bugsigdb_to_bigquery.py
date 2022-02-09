from prefect.run_configs import KubernetesRun

import prefect
from prefect import task, Flow
from prefect.storage import GitHub
import httpx
import io
import pandas
from prefect.tasks.gcp import bigquery
from typing import List
import re

def fix_column_names(column_names: List[str]) -> List[str]:
    """This fixes the column names in the csv file for bigquery

    Args:
        column_names (List[str]): old column names to be replaced

    Returns:
        List[str]: lowercased column names with spaces replaced and starting with character or underline (not number)
    """
    new_cols = []
    for col in column_names:
        if len(re.findall(r"^[0-9]\w+", col)) > 0:
            col = "_" + col
        new_cols.append(col.replace(" ", "_").lower())
    return new_cols

@task
def github_download():
    """Download the csv file from github.
    """
    url = "https://raw.githubusercontent.com/waldronlab/BugSigDBExports/main/full_dump.csv"
    logger = prefect.context.get("logger")
    logger.info(f"downloading from {url}")
    df = pandas.read_csv(url, skiprows = 1)
    df.columns = fix_column_names(df.columns)
    logger.info("fixing column names")
    df.to_csv(r'full_dump_fixed.csv', index=False)
    loader = bigquery.BigQueryLoadFile(file='full_dump_fixed.csv')
    loader.run(
        file='full_dump_fixed.csv',dataset_id='omicidx_etl',
        table='bugsigdb_raw',source_format='CSV',allow_quoted_newlines=True,
        autodetect=True
    )
    logger.info(f"Bigsigdb loaded to 'bugsigdb_raw' in schema 'omicidx_etl'")
        

with Flow("bugsigdb-to-bigquery") as flow:
    github_download()

# Configure extra environment variables for this flow,
# and set a custom image
flow.run_config = KubernetesRun(
     env={"SOME_VAR": ""},
     image="gcr.io/omicidx-338300/prefect-gcp-base"
)

flow.storage = GitHub(repo='seandavi/prefect-flows', path='flows/syncs/bugsigdb_to_bigquery.py')

flow.register('testing')

