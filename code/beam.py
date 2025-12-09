import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery
import csv
import re

# ====================================================
#  CUSTOM PIPELINE OPTIONS (only input!)
# ====================================================
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            required=True,
            help='GCS input CSV path'
        )

# ====================================================
#  GLOBAL CONFIGS
# ====================================================
project_id = "" # Your gcp project id
region = "" 
gcs_bucket = "" # GCS bucket where you store the csv data file 

bq_dataset = "" # Name of teh bigquery dataset

delivered_table = "delivered_orders"
other_table = "other_status_orders"

delivered_table_spec = f"{project_id}:{bq_dataset}.{delivered_table}"
other_table_spec = f"{project_id}:{bq_dataset}.{other_table}"


# ====================================================
#  TRANSFORMATION FUNCTIONS
# ====================================================
def remove_last_colon(row):
    cols = row.split(',')     
    if cols[4].endswith(':'):
        cols[4] = cols[4][:-1]
    return ",".join(cols)


def remove_special_characters(row):
    cols = row.split(',')
    cleaned = [re.sub(r'[?%&]', '', c) for c in cols]
    return ",".join(cleaned)


def print_row(row):
    print(row)
    return row


def to_json(csv_str):
    fields = csv_str.split(',')
    return {
        "customer_id": fields[0],
        "date": fields[1],
        "timestamp": fields[2],
        "order_id": fields[3],
        "items": fields[4],
        "amount": fields[5],
        "mode": fields[6],
        "restaurant": fields[7],
        "status": fields[8],
        "ratings": fields[9],
        "feedback": fields[10],
        "new_col": fields[11]
    }


# BigQuery schema
table_schema = (
    "customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,"
    "items:STRING,amount:STRING,mode:STRING,restaurant:STRING,"
    "status:STRING,ratings:STRING,feedback:STRING,new_col:STRING"
)


# ====================================================
#  PIPELINE RUN
# ====================================================
def run():
    pipeline_options = PipelineOptions()
    custom = pipeline_options.view_as(CustomOptions)

    if not custom.input:
        raise ValueError("❌ Missing --input argument")

    input_path = custom.input

    p = beam.Pipeline(options=pipeline_options)

    # ====================================================
    #  DATA LOADING + CLEANING
    # ====================================================
    cleaned_data = (
        p
        | "Read CSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
        | "Remove last colon" >> beam.Map(remove_last_colon)
        | "Lowercase" >> beam.Map(lambda x: x.lower())
        | "Remove special chars" >> beam.Map(remove_special_characters)
        | "Add new column" >> beam.Map(lambda row: row + ",1")
    )

    # ====================================================
    #  FILTERING BRANCHES
    # ====================================================
    delivered_orders = cleaned_data | "Filter delivered" >> beam.Filter(
        lambda row: row.split(',')[8].lower() == "delivered"
    )

    other_orders = cleaned_data | "Filter others" >> beam.Filter(
        lambda row: row.split(',')[8].lower() != "delivered"
    )

    # ====================================================
    #  COUNTS (LOGGING)
    # ====================================================
    (cleaned_data
     | "Count total" >> beam.combiners.Count.Globally()
     | "Print total" >> beam.Map(lambda x: print(f"Total Count: {x}"))
    )

    (delivered_orders
     | "Count delivered" >> beam.combiners.Count.Globally()
     | "Print delivered" >> beam.Map(lambda x: print(f"Delivered Count: {x}"))
    )

    (other_orders
     | "Count others" >> beam.combiners.Count.Globally()
     | "Print others" >> beam.Map(lambda x: print(f"Other Status Count: {x}"))
    )

    # ====================================================
    #  BIGQUERY DATASET CREATE (safe)
    # ====================================================
    client = bigquery.Client()
    dataset_id = f"{project_id}.{bq_dataset}"

    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Created dataset {dataset_id}")

    # ====================================================
    #  WRITE TO BIGQUERY
    # ====================================================
    (delivered_orders
     | "Delivered to JSON" >> beam.Map(to_json)
     | "Write delivered BQ" >> beam.io.WriteToBigQuery(
            delivered_table_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        )
     )

    (other_orders
     | "Others to JSON" >> beam.Map(to_json)
     | "Write others BQ" >> beam.io.WriteToBigQuery(
            other_table_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        )
     )

    # ====================================================
    #  RUN PIPELINE
    # ====================================================
    result = p.run()
    result.wait_until_finish()
    print("✅ Pipeline finished successfully!")


if __name__ == "__main__":
    run()
