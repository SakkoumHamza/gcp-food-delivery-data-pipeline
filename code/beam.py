import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import re


# ====================================================
#  CUSTOM PIPELINE OPTIONS
# ====================================================

class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input",
            required=True,
            help="GCS path to input CSV file"
        )
        parser.add_argument(
            "--project_id",
            required=True,
            help="GCP project ID"
        )
        parser.add_argument(
            "--bq_dataset",
            required=True,
            help="BigQuery dataset name"
        )


# ====================================================
#  TRANSFORMATION FUNCTIONS
# ====================================================
def remove_last_colon(row):
    cols = row.split(',')
    if len(cols) > 4 and cols[4].endswith(':'):
        cols[4] = cols[4][:-1]
    return ",".join(cols)


def remove_special_characters(row):
    cols = row.split(',')
    cleaned = [re.sub(r'[?%&]', '', c) for c in cols]
    return ",".join(cleaned)


def to_json(csv_str):
    fields = csv_str.split(',')
    if len(fields) < 12:
        return None

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


# ====================================================
#  BIGQUERY SCHEMA
# ====================================================

TABLE_SCHEMA = (
    "customer_id:STRING,"
    "date:STRING,"
    "timestamp:STRING,"
    "order_id:STRING,"
    "items:STRING,"
    "amount:STRING,"
    "mode:STRING,"
    "restaurant:STRING,"
    "status:STRING,"
    "ratings:STRING,"
    "feedback:STRING,"
    "new_col:STRING"
)



# # ====================================================
# #  PIPELINE RUN
# # ====================================================

def run():
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    custom = pipeline_options.view_as(CustomOptions)

    delivered_table = (
        f"{custom.project_id}:{custom.bq_dataset}.delivered_orders"
    )
    other_table = (
        f"{custom.project_id}:{custom.bq_dataset}.other_status_orders"
    )
    # ------------------------------------------------
    #  PIPELINE (context-managed — REQUIRED)
    # ------------------------------------------------
    with beam.Pipeline(options=pipeline_options) as p:

        cleaned_data = (
            p
            | "Read CSV" >> beam.io.ReadFromText(
                custom.input,
                skip_header_lines=1
            )
            | "Remove last colon" >> beam.Map(remove_last_colon)
            | "Lowercase" >> beam.Map(lambda x: x.lower())
            | "Remove special chars" >> beam.Map(remove_special_characters)
            | "Add new column" >> beam.Map(lambda row: row + ",1")
        )

        delivered_orders = (
            cleaned_data
            | "Filter delivered" >> beam.Filter(
                lambda row: row.split(",")[8] == "delivered"
            )
        )

        other_orders = (
            cleaned_data
            | "Filter others" >> beam.Filter(
                lambda row: row.split(",")[8] != "delivered"
            )
        )

        # ------------------------------------------------
        #  METRICS / LOGGING
        # ------------------------------------------------
        (
            cleaned_data
            | "Count total" >> beam.combiners.Count.Globally()
            | "Log total" >> beam.Map(
                lambda x: logging.info(f"Total records: {x}")
            )
        )

        (
            delivered_orders
            | "Count delivered" >> beam.combiners.Count.Globally()
            | "Log delivered" >> beam.Map(
                lambda x: logging.info(f"Delivered records: {x}")
            )
        )

        (
            other_orders
            | "Count others" >> beam.combiners.Count.Globally()
            | "Log others" >> beam.Map(
                lambda x: logging.info(f"Other status records: {x}")
            )
        )

        # ------------------------------------------------
        #  WRITE TO BIGQUERY
        # ------------------------------------------------
        (
            delivered_orders
            | "Delivered to JSON" >> beam.Map(to_json)
            | "Write delivered BQ" >> beam.io.WriteToBigQuery(
                delivered_table,
                schema=TABLE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters={
                    "timePartitioning": {"type": "DAY"}
                },
            )
        )

        (
            other_orders
            | "Others to JSON" >> beam.Map(to_json)
            | "Write others BQ" >> beam.io.WriteToBigQuery(
                other_table,
                schema=TABLE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters={
                    "timePartitioning": {"type": "DAY"}
                },
            )
        )


# ====================================================
#  ENTRY POINT
# ====================================================
if __name__ == "__main__":
    run()
