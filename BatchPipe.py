#install necessary packages
!pip install apache-beam
!pip install google-apitools

#batch pipeline code

from apache_beam.io.gcp.bigquery import WriteToBigQuery
import argparse
import os
from datetime import date
from google.cloud import bigquery
path1='consummate-yew-336502-df73d9107874.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=path1
tab_id='tp.fareAmount'
tab_id2='tp.tipAmount'
tab_id3='tp.tollsAmount'
tab_id1='tp.totalAmount'
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg')
args, beam_args = parser.parse_known_args()

# Create and set your Pipeline Options.
beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    service_account_email='demo-service-account@consummate-yew-336502.iam.gserviceaccount.com',
    region='us-central1',
    project='consummate-yew-336502',
     stage_location='gs://anish-12345/stage'
    )

client = bigquery.Client()

dataset_id = "consummate-yew-336502.tp"

try:
  client.get_dataset(dataset_id)

except:
  dataset = bigquery.Dataset(dataset_id)  #

  dataset.location = "US"
  dataset.description = "dataset"

  dataset_ref = client.create_dataset(dataset)
def to_json(fields):
    json_str = {"vendor_id":fields[0],
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
                 "dropoff_location_id":fields[16]
                 }
    return json_str
schema_1='vendor_id:INTEGER,pickup_datetime:TIMESTAMP,dropoff_datetime:TIMESTAMP,passenger_count:INTEGER,trip_distance:FLOAT,rate_code:INTEGER,store_and_fwd_flag:BOOLEAN,payment_type:INTEGER,fare_amount:FLOAT,extra:FLOAT,mta_tax:FLOAT,tip_amount:FLOAT,tolls_amount:FLOAT,imp_surcharge:FLOAT,total_amount:FLOAT,pickup_location_id:INTEGER,dropoff_location_id:INTEGER'


with beam.Pipeline(options=beam_options) as pipeline:
    data_ingestion=(
    pipeline
    | beam.io.ReadFromText('gs://anish-12345/nyc_taxi.csv',skip_header_lines=1)
    | beam.Map(lambda x:x.split(','))
    | "filter total amount greater than 40">> beam.Filter(lambda x:float(x[14])>40)
    | "convert to json2">> beam.Map(to_json)
    | "write to total amount table">> beam.io.WriteToBigQuery(
        tab_id1,
        schema=schema_1,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
    )