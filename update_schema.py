import yaml

from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    filename="./cred/malaysia-stock-research-ac9157bd12e0.json"
)
bq_client = bigquery.Client(credentials=credentials)

with open("./src/columns_desc.yaml") as g:
    col_desc = yaml.load(g, Loader=yaml.FullLoader)
table = bq_client.get_table(
    table="malaysia-stock-research.malaysia_derivatives.eod_data"
)
schema = [
    bigquery.SchemaField(
        name=field.name,
        field_type=field.field_type,
        mode=field.mode,
        description=col_desc[f"{field.name}"],
    )
    for field in table.schema
]
table.schema = schema
bq_client.update_table(table, ["schema"])

print("Schema updated.")
