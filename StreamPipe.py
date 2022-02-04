from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import os
import time
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date
from google.cloud import bigquery
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly

path1 = 'consummate-yew-336502-df73d9107874.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path1

tab_id1 = 'airflow5.total_amount'
import apache_beam as beam

# from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg')
args, beam_args = parser.parse_known_args()

# Create and set your Pipeline Options.
beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    # temp_location='gs://us-central1-c1-dea909ea-bucket/temp1',
    service_account_email='demo-service-account@consummate-yew-336502.iam.gserviceaccount.com',
    region='us-central1',
    project='consummate-yew-336502',
    stage_location='gs://anish_1998/test'
)

beam_options.view_as(StandardOptions).streaming = True
client = bigquery.Client()

dataset_id = "consummate-yew-336502.airflow5"

try:
    client.get_dataset(dataset_id)

except:
    dataset = bigquery.Dataset(dataset_id)  #

    dataset.location = "US"
    dataset.description = "dataset"

    dataset_ref = client.create_dataset(dataset)


def decoding(e):
    e = e.decode('utf-8')
    return e[:-2]


class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        yield TimestampedValue(element)


input_subscription = 'projects/consummate-yew-336502/subscriptions/demo-topic-sub'


def to_json(fields):
    json_str = {"vendor_id": fields[0],
                "pickup_datetime": fields[1],
                "dropoff_datetime": fields[2],
                "passenger_count": fields[3],
                "trip_distance": fields[4],
                "rate_code": fields[5],
                "store_and_fwd_flag": fields[6],
                "payment_type": fields[7],
                "fare_amount": fields[8],
                "extra": fields[9],
                "mta_tax": fields[10],
                "tip_amount": fields[11],
                "tolls_amount": fields[12],
                "imp_surcharge": fields[13],
                "total_amount": fields[14],
                "pickup_location_id": fields[15],
                "dropoff_location_id": fields[16]
                }
    return json_str


schema_1 = 'vendor_id:INTEGER,pickup_datetime:TIMESTAMP,dropoff_datetime:TIMESTAMP,passenger_count:INTEGER,trip_distance:FLOAT,rate_code:INTEGER,store_and_fwd_flag:BOOLEAN,payment_type:INTEGER,fare_amount:FLOAT,extra:FLOAT,mta_tax:FLOAT,tip_amount:FLOAT,tolls_amount:FLOAT,imp_surcharge:FLOAT,total_amount:FLOAT,pickup_location_id:INTEGER,dropoff_location_id:INTEGER'

with beam.Pipeline(options=beam_options) as pipeline:
    data_ingestion = (
            pipeline
            | beam.io.ReadFromPubSub(subscription=input_subscription, timestamp_attribute=None)
            | beam.Map(decoding)
            | beam.Map(lambda x: x.split(','))
            | beam.Map(to_json)
            | beam.WindowInto(beam.window.FixedWindows(5))

            # | "convert to json2">> beam.Map(to_json)
            | beam.io.WriteToBigQuery(
        tab_id1,
        schema=schema_1,
        method=
        beam.io.WriteToBigQuery.Method.FILE_LOADS,
        triggering_frequency=1,
        write_disposition=
        beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=
        beam.io.BigQueryDisposition.CREATE_IF_NEEDED

    )
    )