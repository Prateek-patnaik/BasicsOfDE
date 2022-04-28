#Import KafkaConsumer from Kafka library
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
import sys
import json
from json import loads
from pyspark.sql.functions import *

import csv
bootstrap_servers = ['localhost:9092']
topicName = 'mytopic2'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id1',bootstrap_servers = bootstrap_servers,
                          auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest

data=[]
field_names=['name','address','sum_x1','sum_x2','sum_x3','sum_x4']
## Reading the message from consumer
try:
    for message in consumer:
        print(message.value)
        data.append(message.value)
    with open('test_kafka.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(data)
except KeyboardInterrupt:
    sys.exit()



