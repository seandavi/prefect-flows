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
from google.cloud import bigquery


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
    """Download the csv file from github."""

    url = "https://raw.githubusercontent.com/waldronlab/BugSigDBExports/main/full_dump.csv"
    logger = prefect.context.get("logger")
    logger.info(f"downloading from {url}")
    df = pandas.read_csv(url, skiprows=1)
    df.columns = fix_column_names(df.columns)
    logger.info("fixing column names")
    df.to_csv(r"full_dump_fixed.csv", index=False)
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "omicidx_etl.bugsigdb_raw"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        allow_quoted_newlines=True,
    )

    file_path = "full_dump_fixed.csv"

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


with Flow("bugsigdb-to-bigquery") as flow:
    github_download()

# Configure extra environment variables for this flow,
# and set a custom image
flow.run_config = KubernetesRun(
    env={"SOME_VAR": ""}, image="gcr.io/omicidx-338300/prefect-gcp-base"
)

flow.storage = GitHub(
    repo="seandavi/prefect-flows", path="flows/syncs/bugsigdb_to_bigquery.py"
)

flow.register("testing")
