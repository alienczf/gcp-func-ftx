import datetime as dt
import pytz
from google.cloud import bigquery
from ftx.utils import getPcf, getRebalances


def on_pubsub(event, context):
    client = bigquery.Client()
    client.load_table_from_dataframe(
        getPcf().rename({"name": "sym"}, axis=1),
        "jkjk-339310.FTX.LtPcf",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"]
        ))
    print('uploaded pcf')
    client.load_table_from_dataframe(
        getRebalances(getAllToken()),
        "jkjk-339310.FTX.LtRebal",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ))
    print('uploaded rebal')
    print(f'successfully uploaded leveraged token data {dt.datetime.now(tz=pytz.utc)}')
