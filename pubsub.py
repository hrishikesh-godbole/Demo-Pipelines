#install necessary packages
!pip install apache-beam[gcp]

!pip install google-apitools

import os
path = r'consummate-yew-336502-df73d9107874.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

import time
from google.cloud import pubsub_v1
project = "consummate-yew-336502"
pubsub_topic = "projects/consummate-yew-336502/topics/demo-topic"
input_file = "nyc_taxi.csv"
publisher = pubsub_v1.PublisherClient()
with open(input_file, 'rb') as ifp:
        header = ifp.readline()
        for line in ifp:
            event_data = line
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)